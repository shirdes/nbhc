package com.urbanairship.hbase.shc;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.urbanairship.hbase.shc.dispatch.RegionServerDispatcher;
import com.urbanairship.hbase.shc.operation.Operation;
import com.urbanairship.hbase.shc.operation.VoidResponseParser;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import java.lang.reflect.Method;

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

        return dispatcher.request(new Operation<Get, Result>(
                location,
                get,
                GET_TARGET_METHOD,
                TARGET_PROTOCOL,
                GET_RESPONSE_PARSER
        ));
    }

    public ListenableFuture<Void> put(String table, Put put) {
        HRegionLocation location = topology.getRegionServer(table, put.getRow());
        return dispatcher.request(new Operation<Put, Void>(
                location,
                put,
                PUT_TARGET_METHOD,
                TARGET_PROTOCOL,
                PUT_RESPONSE_PARSER
        ));
    }

    public ListenableFuture<Void> delete(String table, Delete delete) {
        HRegionLocation location = topology.getRegionServer(table, delete.getRow());
        return dispatcher.request(new Operation<Delete, Void>(
                location,
                delete,
                DELETE_TARGET_METHOD,
                TARGET_PROTOCOL,
                DELETE_RESPONSE_PARSER
        ));
    }

}
