package com.urbanairship.hbase.shc;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.urbanairship.hbase.shc.response.ResponseError;
import com.urbanairship.hbase.shc.dispatch.HbaseOperationResultFuture;
import com.urbanairship.hbase.shc.dispatch.RegionServerDispatcher;
import com.urbanairship.hbase.shc.operation.Operation;
import com.urbanairship.hbase.shc.operation.VoidResponseParser;
import com.urbanairship.hbase.shc.response.MultiResponseParser;
import com.urbanairship.hbase.shc.response.ResponseCallback;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.ipc.RemoteException;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class HbaseClient {

    private static final Class<? extends VersionedProtocol> TARGET_PROTOCOL = HRegionInterface.class;

    private static final Method GET_TARGET_METHOD = loadTargetMethod("get", new Class[]{byte[].class, Get.class});
    private static final Function<HbaseObjectWritable, Result> GET_RESPONSE_PARSER = new Function<HbaseObjectWritable, Result>() {
        @Override
        public Result apply(HbaseObjectWritable value) {
            Object result = value.get();
            if (!(result instanceof Result)) {
                throw new RuntimeException(String.format("Expected response value of %s but received %s for 'get' operation",
                        Result.class.getName(), result.getClass().getName()));
            }

            return (Result) result;
        }
    };

    private static final Method PUT_TARGET_METHOD = loadTargetMethod("put", new Class[]{byte[].class, Put.class});
    private static final Function<HbaseObjectWritable, Void> PUT_RESPONSE_PARSER = new VoidResponseParser("put");

    private static final Method DELETE_TARGET_METHOD = loadTargetMethod("delete", new Class[]{byte[].class, Delete.class});
    private static final Function<HbaseObjectWritable, Void> DELETE_RESPONSE_PARSER = new VoidResponseParser("delete");

    private static final Method MULTI_ACTION_TARGET_METHOD = loadTargetMethod("multi", new Class[]{MultiAction.class});

    private static Method loadTargetMethod(String methodName, Class<?>[] params) {
        try {
            return HRegionServer.class.getMethod(methodName, params);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(String.format("Unable to load target method for '%s' operation", methodName));
        }
    }

    private final RegionServerDispatcher dispatcher;
    private final RegionOwnershipTopology topology;

    public HbaseClient(RegionServerDispatcher dispatcher, RegionOwnershipTopology topology) {
        this.dispatcher = dispatcher;
        this.topology = topology;
    }

    public ListenableFuture<Result> get(String table, Get get) {
        HRegionLocation location = topology.getRegionServer(table, get.getRow());
        // TODO: think that this needs to be linked in a loop that retries in the case where we get some
        // TODO: response from the server indicating that the row is not there and then we need to lookup the
        // TODO: region again and retry...

        return makeSingleOperationRequest(GET_TARGET_METHOD, location, get, GET_RESPONSE_PARSER);
    }

    public ListenableFuture<Void> put(String table, Put put) {
        HRegionLocation location = topology.getRegionServer(table, put.getRow());
        return makeSingleOperationRequest(PUT_TARGET_METHOD, location, put, PUT_RESPONSE_PARSER);
    }

    public ListenableFuture<Void> multiPut(String table, List<Put> puts) {
        return makeMultiMutationRequest(table, puts, "put");
    }

    public ListenableFuture<Void> delete(String table, Delete delete) {
        HRegionLocation location = topology.getRegionServer(table, delete.getRow());
        return makeSingleOperationRequest(DELETE_TARGET_METHOD, location, delete, DELETE_RESPONSE_PARSER);
    }

    public ListenableFuture<Void> multiDelete(String table, List<Delete> deletes) {
        return makeMultiMutationRequest(table, deletes, "delete");
    }

    // TODO: this method sucks
    private <M extends Row> ListenableFuture<Void> makeMultiMutationRequest(String table,
                                                                            List<M> mutations,
                                                                            String operationName) {
        Map<HRegionLocation, MultiAction<M>> regionActions = Maps.newHashMap();
        for (int i = 0; i < mutations.size(); i++) {
            M mutation = mutations.get(i);

            HRegionLocation location = topology.getRegionServer(table, mutation.getRow());
            MultiAction<M> regionAction = regionActions.get(location);
            if (regionAction == null) {
                regionAction = new MultiAction<M>();
                regionActions.put(location, regionAction);
            }

            regionAction.add(location.getRegionInfo().getRegionName(), new Action<M>(mutation, i));
        }

        List<ListenableFuture<Void>> futures = Lists.newArrayList();
        for (HRegionLocation location : regionActions.keySet()) {
            HbaseOperationResultFuture<Void> future = new HbaseOperationResultFuture<Void>();
            MultiResponseParser callback = new MultiResponseParser(future, operationName);

            MultiAction<M> action = regionActions.get(location);
            Invocation invocation = new Invocation(MULTI_ACTION_TARGET_METHOD, TARGET_PROTOCOL, new Object[]{action});

            Operation operation = new Operation(getHost(location), invocation);

            // TODO: going to need to hold these and ensure that we remove all the callbacks on timeout
            int requestId = dispatcher.request(operation, callback);

            futures.add(future);
        }

        // TODO: don't like this....
        ListenableFuture<List<Void>> merged = Futures.allAsList(futures);
        return Futures.transform(merged, Functions.<Void>constant(null));
    }

    private <P, R> ListenableFuture<R> makeSingleOperationRequest(Method targetMethod,
                                                                  HRegionLocation location,
                                                                  P param,
                                                                  final Function<HbaseObjectWritable, R> responseParser) {

        final HbaseOperationResultFuture<R> future = new HbaseOperationResultFuture<R>();
        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void receiveResponse(HbaseObjectWritable value) {
                R response;
                try {
                    response = responseParser.apply(value);
                }
                catch (Exception e) {
                    future.communicateError(e);
                    return;
                }

                future.communicateResult(response);
            }

            @Override
            public void receiveError(ResponseError errorResponse) {
                // TODO; get smarter about the error handling
                future.communicateError(new RemoteException(errorResponse.getErrorClass(),
                        errorResponse.getErrorMessage().isPresent() ? errorResponse.getErrorMessage().get() : ""));
            }
        };

        Invocation invocation = new Invocation(targetMethod, TARGET_PROTOCOL, new Object[]{
                location.getRegionInfo().getRegionName(),
                param
        });

        Operation operation = new Operation(getHost(location), invocation);

        // TODO: should carry the request id into the future or have the future tell us when a timeout occurs
        // TODO: so the callback can be removed.  Kind of messy to bleed that all the way up here...
        // TODO: should find a way to do it lower or something
        int requestId = dispatcher.request(operation, callback);

        return future;
    }

    private HostAndPort getHost(HRegionLocation location) {
        return HostAndPort.fromParts(location.getHostname(), location.getPort());
    }

}
