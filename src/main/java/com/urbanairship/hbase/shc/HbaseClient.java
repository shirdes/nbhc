package com.urbanairship.hbase.shc;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.urbanairship.hbase.shc.dispatch.HbaseOperationResultFuture;
import com.urbanairship.hbase.shc.dispatch.RegionServerDispatcher;
import com.urbanairship.hbase.shc.dispatch.RequestManager;
import com.urbanairship.hbase.shc.dispatch.ResultBroker;
import com.urbanairship.hbase.shc.operation.Operation;
import com.urbanairship.hbase.shc.operation.VoidResponseParser;
import com.urbanairship.hbase.shc.response.MultiResponseParser;
import com.urbanairship.hbase.shc.response.RemoteError;
import com.urbanairship.hbase.shc.response.ResponseCallback;
import com.urbanairship.hbase.shc.response.ScannerBatchResult;
import com.urbanairship.hbase.shc.response.ScannerResultStream;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

    private static final Method OPEN_SCANNER_TARGET_METHOD = loadTargetMethod("openScanner", new Class[]{byte[].class, Scan.class});
    private static final Function<HbaseObjectWritable, Long> OPEN_SCANNER_RESPONSE_PARSER = new Function<HbaseObjectWritable, Long>() {
        @Override
        public Long apply(HbaseObjectWritable value) {
            Object object = value.get();
            if (!(object instanceof Long)) {
                throw new RuntimeException(String.format("Expected response value of %s but received %s for 'open scanner' operation",
                        Long.class.getName(), object.getClass().getName()));
            }

            return (Long) object;
        }
    };

    private static final Method CLOSE_SCANNER_TARGET_METHOD = loadTargetMethod("close", new Class[]{Long.TYPE});
    private static final Function<HbaseObjectWritable, Void> CLOSE_SCANNER_RESPONSE_PARSER = new VoidResponseParser("close scanner");

    private static final Method SCANNER_NEXT_TARGET_METHOD = loadTargetMethod("next", new Class[]{Long.TYPE, Integer.TYPE});

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
    private final RequestManager requestManager;
    private final int maxRetries;

    public HbaseClient(RegionServerDispatcher dispatcher, RegionOwnershipTopology topology, RequestManager requestManager, int maxRetries) {
        this.dispatcher = dispatcher;
        this.topology = topology;
        this.requestManager = requestManager;
        this.maxRetries = maxRetries;
    }

    public ListenableFuture<Result> get(String table, Get get) {
        return singleRowRequest(table, get, GET_TARGET_METHOD, GET_RESPONSE_PARSER);
    }

    public ListenableFuture<List<Result>> multiGet(String table, final List<Get> gets) {
        Map<HRegionLocation, MultiAction<Get>> locationActions = groupByLocation(table, gets);

        // TODO: this is a complete and total mess!
        List<ListenableFuture<ImmutableMap<Integer, Result>>> futures = Lists.newArrayList();
        for (HRegionLocation location : locationActions.keySet()) {
            MultiAction<Get> multiAction = locationActions.get(location);

            Invocation invocation = new Invocation(MULTI_ACTION_TARGET_METHOD, TARGET_PROTOCOL, new Object[] {multiAction});
            Operation operation = new Operation(getHost(location), invocation);

            final HbaseOperationResultFuture<ImmutableMap<Integer, Result>> future =
                    new HbaseOperationResultFuture<ImmutableMap<Integer, Result>>(requestManager);

            final ConcurrentMap<Integer, Result> collectedResults = new ConcurrentHashMap<Integer, Result>();
            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void receiveResponse(HbaseObjectWritable value) {
                    Object responseObject = value.get();
                    if (!(responseObject instanceof MultiResponse)) {
                        // TODO: should probably bomb out
                        return;
                    }

                    MultiResponse response = (MultiResponse) responseObject;

                    Map<Integer, Get> needRetry = Maps.newHashMap();
                    Iterable<Pair<Integer, Object>> pairs = Iterables.concat(response.getResults().values());
                    for (Pair<Integer, Object> pair : pairs) {
                        Object result = pair.getSecond();
                        if (result == null || result instanceof Throwable) {
                            // TODO: need to do a check to see if the error can be retried.  Shouldn't retry in the case of a DoNotRetryIOException
                            needRetry.put(pair.getFirst(), gets.get(pair.getFirst()));
                        }
                        else if (result instanceof Result) {
                            collectedResults.put(pair.getFirst(), (Result) result);
                        }
                        else {
                            future.communicateError(new RuntimeException("Received unknown response object of type " +
                                    result.getClass()));
                            // TODO: this return is real deep.
                            return;
                        }
                    }

                    if (needRetry.isEmpty()) {
                        future.communicateResult(ImmutableMap.copyOf(collectedResults));
                    }
                }

                @Override
                public void receiveError(RemoteError responseError) {
                    // TODO: check if the error is retryable and get smarter about what we return perhaps
                    future.communicateError(new RemoteException(responseError.getErrorClass(),
                            responseError.getErrorMessage().isPresent() ? responseError.getErrorMessage().get() : ""));
                }
            };

            int requestId = dispatcher.request(operation, callback);
            future.setCurrentActiveRequestId(requestId);

            futures.add(future);
        }

        ListenableFuture<List<ImmutableMap<Integer, Result>>> all = Futures.allAsList(futures);
        return Futures.transform(all, new Function<List<ImmutableMap<Integer, Result>>, List<Result>>() {
            @Override
            public List<Result> apply(List<ImmutableMap<Integer, Result>> collected) {
                SortedMap<Integer, Result> merged = Maps.newTreeMap();
                for (ImmutableMap<Integer, Result> batch : collected) {
                    merged.putAll(batch);
                }

                return Lists.newArrayList(merged.values());
            }
        });
    }

    public ListenableFuture<Void> put(String table, Put put) {
        return singleRowRequest(table, put, PUT_TARGET_METHOD, PUT_RESPONSE_PARSER);
    }

    public ListenableFuture<Void> multiPut(String table, List<Put> puts) {
        return makeMultiMutationRequest(table, puts, "put");
    }

    public ListenableFuture<Void> delete(String table, Delete delete) {
        return singleRowRequest(table, delete, DELETE_TARGET_METHOD, DELETE_RESPONSE_PARSER);
    }

    public ListenableFuture<Void> multiDelete(String table, List<Delete> deletes) {
        return makeMultiMutationRequest(table, deletes, "delete");
    }

    public ScannerResultStream getScannerStream(String table, Scan scan) {
        // TODO: ensure that the Scan object has a value set for caching?  Otherwise we'd have to have a default given
        // TODO: to this client as a data member?

        return null;
    }

    private ListenableFuture<Long> openScanner(HRegionLocation location, String table, Scan scan) {
        // TODO: implement
        return null;
    }

    private ListenableFuture<Void> closeScanner(HRegionLocation location, long scannerId) {
        // TODO: implement
        return null;
    }

    // TODO: don't like having a method with two numbers right next to each other as params. Perhaps next batch identifier object or something?
    private ListenableFuture<ScannerBatchResult> getScannerNextBatch(HRegionLocation location,
                                                                     long scannerId,
                                                                     int numResults) {

        final HbaseOperationResultFuture<ScannerBatchResult> future = new HbaseOperationResultFuture<ScannerBatchResult>(requestManager);
        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void receiveResponse(HbaseObjectWritable value) {
                Object object = value.get();
                if (object == null) {
                    future.communicateResult(ScannerBatchResult.finished());
                    return;
                }

                if (!(object instanceof Result[])) {
                    future.communicateError(new RuntimeException("Received result in scanner 'next' call that was not a Result[]"));
                    return;
                }

                Result[] results = (Result[]) object;
                ScannerBatchResult batchResult = (results.length == 0)
                        ? ScannerBatchResult.noMoreResultsInRegion()
                        : ScannerBatchResult.resultsReturned(ImmutableList.copyOf(results));

                future.communicateResult(batchResult);
            }

            @Override
            public void receiveError(RemoteError errorResponse) {
                // TODO: stop sucking at the errors.
                // TODO: gonna need to check for things like the region being offline or the scanner expiring and
                // TODO:  in those cases do some retry logic
                future.communicateError(new RemoteException(errorResponse.getErrorClass(),
                        errorResponse.getErrorMessage().isPresent() ? errorResponse.getErrorMessage().get() : ""));
            }
        };

        Invocation invocation = new Invocation(SCANNER_NEXT_TARGET_METHOD, TARGET_PROTOCOL, new Object[]{scannerId, numResults});
        Operation operation = new Operation(getHost(location), invocation);
        int requestId = dispatcher.request(operation, callback);
        future.setCurrentActiveRequestId(requestId);

        return future;
    }

    private <P extends Row, R> ListenableFuture<R> singleRowRequest(final String table,
                                                                    final P param,
                                                                    Method targetMethod,
                                                                    Function<HbaseObjectWritable, R> responseParser) {

        HRegionLocation location = topology.getRegionServer(table, param.getRow());

        HbaseOperationResultFuture<R> future = new HbaseOperationResultFuture<R>(requestManager);

        Invocation invocation = new Invocation(targetMethod, TARGET_PROTOCOL, new Object[]{
                location.getRegionInfo().getRegionName(),
                param
        });

        Operation operation = new Operation(getHost(location), invocation);
        sendWithRetries(operation, responseParser, future, new Supplier<HRegionLocation>() {
            @Override
            public HRegionLocation get() {
                return topology.getRegionServerNoCache(table, param.getRow());
            }
        });

        return future;
    }

    // TODO: this method sucks
    private <M extends Row> ListenableFuture<Void> makeMultiMutationRequest(String table,
                                                                            List<M> mutations,
                                                                            String operationName) {

        Map<HRegionLocation, MultiAction<M>> regionActions = groupByLocation(table, mutations);

        List<ListenableFuture<Void>> futures = Lists.newArrayList();
        for (HRegionLocation location : regionActions.keySet()) {
            HbaseOperationResultFuture<Void> future = new HbaseOperationResultFuture<Void>(requestManager);
            MultiResponseParser callback = new MultiResponseParser(future, operationName);

            MultiAction<M> action = regionActions.get(location);
            Invocation invocation = new Invocation(MULTI_ACTION_TARGET_METHOD, TARGET_PROTOCOL, new Object[]{action});

            Operation operation = new Operation(getHost(location), invocation);

            // TODO: going to need to hold these and ensure that we remove all the callbacks on timeout
            int requestId = dispatcher.request(operation, callback);
            future.setCurrentActiveRequestId(requestId);

            futures.add(future);
        }

        // TODO: don't like this....
        ListenableFuture<List<Void>> merged = Futures.allAsList(futures);
        return Futures.transform(merged, Functions.<Void>constant(null));
    }

    private <O extends Row> Map<HRegionLocation, MultiAction<O>> groupByLocation(String table,
                                                                                 List<O> operations) {
        Map<HRegionLocation, MultiAction<O>> regionActions = Maps.newHashMap();
        for (int i = 0; i < operations.size(); i++) {
            O operation = operations.get(i);

            HRegionLocation location = topology.getRegionServer(table, operation.getRow());
            MultiAction<O> regionAction = regionActions.get(location);
            if (regionAction == null) {
                regionAction = new MultiAction<O>();
                regionActions.put(location, regionAction);
            }

            regionAction.add(location.getRegionInfo().getRegionName(), new Action<O>(operation, i));
        }

        return regionActions;
    }

    private <R> void sendWithRetries(final Operation operation,
                                     final Function<HbaseObjectWritable, R> responseParser,
                                     final ResultBroker<R> resultBroker,
                                     final Supplier<HRegionLocation> updatedLocationSupplier) {

        final SimpleResponseParsingResponseHandler<R> responseHandler =
                new SimpleResponseParsingResponseHandler<R>(resultBroker, responseParser);

        HbaseRemoteErrorHandler retryWithUpdatedLocationErrorHandler = new HbaseRemoteErrorHandler() {
            @Override
            public void handle(RemoteError error, int attempt) {
                if (isRetryableError(error) && attempt <= maxRetries) {
                    HRegionLocation updatedLocation = updatedLocationSupplier.get();

                    Operation retryOperation = new Operation(getHost(updatedLocation), operation.getInvocation());
                    sendRequest(retryOperation, resultBroker, responseHandler, this, attempt + 1);
                }
                else {
                    // TODO; get smarter about the error handling
                    resultBroker.communicateError(new RemoteException(error.getErrorClass(),
                            error.getErrorMessage().isPresent() ? error.getErrorMessage().get() : ""));
                }
            }
        };

        sendRequest(operation, resultBroker, responseHandler, retryWithUpdatedLocationErrorHandler, 1);
    }

    private void sendRequest(Operation operation,
                             ResultBroker<?> resultBroker,
                             final HbaseResponseHandler responseHandler,
                             final HbaseRemoteErrorHandler remoteErrorHandler,
                             final int attempt) {

        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void receiveResponse(HbaseObjectWritable value) {
                responseHandler.handle(value);
            }

            @Override
            public void receiveError(RemoteError remoteError) {
                remoteErrorHandler.handle(remoteError, attempt);
            }
        };

        int requestId = dispatcher.request(operation, callback);
        resultBroker.setCurrentActiveRequestId(requestId);
    }

    private boolean isRetryableError(RemoteError errorResponse) {
        return false;
    }

    private HostAndPort getHost(HRegionLocation location) {
        return HostAndPort.fromParts(location.getHostname(), location.getPort());
    }

    private static interface HbaseResponseHandler {
        void handle(HbaseObjectWritable value);
    }

    private static final class SimpleResponseParsingResponseHandler<R> implements HbaseResponseHandler {

        private final ResultBroker<R> resultBroker;
        private final Function<HbaseObjectWritable, R> responseParser;

        private SimpleResponseParsingResponseHandler(ResultBroker<R> resultBroker, Function<HbaseObjectWritable, R> responseParser) {
            this.resultBroker = resultBroker;
            this.responseParser = responseParser;
        }

        @Override
        public void handle(HbaseObjectWritable value) {
            R response;
            try {
                response = responseParser.apply(value);
            }
            catch (Exception e) {
                resultBroker.communicateError(e);
                return;
            }

            resultBroker.communicateResult(response);
        }
    }

    private static interface HbaseRemoteErrorHandler {
        void handle(RemoteError error, int attempt);
    }

}
