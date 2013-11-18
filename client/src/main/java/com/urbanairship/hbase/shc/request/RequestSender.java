package com.urbanairship.hbase.shc.request;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.urbanairship.hbase.shc.Column;
import com.urbanairship.hbase.shc.ColumnCheck;
import com.urbanairship.hbase.shc.Operation;
import com.urbanairship.hbase.shc.dispatch.HbaseOperationResultFuture;
import com.urbanairship.hbase.shc.dispatch.RegionServerDispatcher;
import com.urbanairship.hbase.shc.dispatch.RequestManager;
import com.urbanairship.hbase.shc.dispatch.ResultBroker;
import com.urbanairship.hbase.shc.response.RemoteError;
import com.urbanairship.hbase.shc.response.ResponseCallback;
import com.urbanairship.hbase.shc.topology.RegionOwnershipTopology;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;

import java.lang.reflect.Method;

import static com.urbanairship.hbase.shc.Protocol.TARGET_PROTOCOL;

public class RequestSender {

    public static final Function<Row, byte[]> ROW_OPERATION_ROW_EXRACTOR = new Function<Row, byte[]>() {
        @Override
        public byte[] apply(Row operation) {
            return operation.getRow();
        }
    };

    private final RegionServerDispatcher dispatcher;
    private final RegionOwnershipTopology topology;
    private final RequestManager requestManager;

    public RequestSender(RegionServerDispatcher dispatcher,
                         RegionOwnershipTopology topology,
                         RequestManager requestManager) {
        this.dispatcher = dispatcher;
        this.topology = topology;
        this.requestManager = requestManager;
    }

    public <A extends Row> ListenableFuture<Boolean> checkedAction(String table,
                                                                   final Method targetMethod,
                                                                   final ColumnCheck check,
                                                                   final A action,
                                                                   Function<HbaseObjectWritable, Boolean> responseParser,
                                                                   int maxRetries) {

        final Column column = check.getColumn();
        Function<HRegionLocation, Invocation> invocationBuilder = new Function<HRegionLocation, Invocation>() {
            @Override
            public Invocation apply(HRegionLocation location) {
                return new Invocation(targetMethod, TARGET_PROTOCOL, new Object[] {
                        location.getRegionInfo().getRegionName(),
                        column.getRow(),
                        column.getFamily(),
                        column.getQualifier(),
                        (check.getValue().isPresent()) ? check.getValue().get() : null,
                        action
                });
            }
        };

        return singleRowRequest(table, action, invocationBuilder, responseParser, maxRetries);
    }

    public <A extends Row, R> ListenableFuture<R> singleRowRequest(String table,
                                                                   A action,
                                                                   Function<HRegionLocation, Invocation> invocationBuilder,
                                                                   Function<HbaseObjectWritable, R> responseParser,
                                                                   int maxRetries) {

        SimpleParseResponseProcessor<R> processor = new SimpleParseResponseProcessor<R>(responseParser);
        return singleActionRequest(table, action, ROW_OPERATION_ROW_EXRACTOR, invocationBuilder, processor, maxRetries);
    }

    public <P, R> ListenableFuture<R> singleActionRequest(final String table,
                                                          final P param,
                                                          final Function<? super P, byte[]> rowExtractor,
                                                          Function<HRegionLocation, Invocation> invocationBuilder,
                                                          ResponseProcessor<R> responseProcessor,
                                                          int maxRetries) {

        HRegionLocation location = topology.getRegionServer(table, rowExtractor.apply(param));

        Supplier<HRegionLocation> updatedLocationSupplier = new Supplier<HRegionLocation>() {
            @Override
            public HRegionLocation get() {
                return topology.getRegionServerNoCache(table, rowExtractor.apply(param));
            }
        };

        Invocation invocation = invocationBuilder.apply(location);

        HbaseOperationResultFuture<R> future = new HbaseOperationResultFuture<R>(requestManager);
        DefaultRequestController<R> controller = new DefaultRequestController<R>(
                location,
                future,
                invocationBuilder,
                responseProcessor,
                updatedLocationSupplier,
                this,
                maxRetries
        );

        sendRequest(location, invocation, future, controller, 1);

        return future;
    }

    public void sendRequest(HRegionLocation location,
                            Invocation invocation,
                            ResultBroker<?> resultBroker,
                            final RequestController controller,
                            final int attempt) {

        Operation operation = new Operation(getHost(location), invocation);

        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void receiveResponse(HbaseObjectWritable value) {
                controller.handleResponse(value);
            }

            @Override
            public void receiveRemoteError(RemoteError remoteError) {
                controller.handleRemoteError(remoteError, attempt);
            }

            @Override
            public void receiveLocalError(Throwable error) {
                controller.handleLocalError(error, attempt);
            }
        };

        int requestId = dispatcher.request(operation, callback);
        resultBroker.setCurrentActiveRequestId(requestId);
    }

    private HostAndPort getHost(HRegionLocation location) {
        return HostAndPort.fromParts(location.getHostname(), location.getPort());
    }
}
