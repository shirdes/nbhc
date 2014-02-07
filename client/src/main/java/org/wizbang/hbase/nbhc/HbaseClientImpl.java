package org.wizbang.hbase.nbhc;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.wizbang.hbase.nbhc.dispatch.HbaseOperationResultFuture;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.request.RequestDetailProvider;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.request.SingleActionController;
import org.wizbang.hbase.nbhc.request.multi.MultiActionCoordinator;
import org.wizbang.hbase.nbhc.request.scan.ScanCoordinator;
import org.wizbang.hbase.nbhc.request.scan.ScannerResultStream;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

import java.lang.reflect.Method;

import static org.wizbang.hbase.nbhc.Protocol.*;

public class HbaseClientImpl implements HbaseClient {

    public static final Function<Row, byte[]> ROW_OPERATION_ROW_EXRACTOR = new Function<Row, byte[]>() {
        @Override
        public byte[] apply(Row operation) {
            return operation.getRow();
        }
    };

    private final RegionOwnershipTopology topology;
    private final RequestSender sender;
    private final RequestManager requestManager;
    private final RetryExecutor retryExecutor;
    private final HbaseClientConfiguration config;

    public HbaseClientImpl(RegionOwnershipTopology topology,
                           RequestSender sender,
                           RequestManager requestManager,
                           RetryExecutor retryExecutor,
                           HbaseClientConfiguration config) {
        this.topology = topology;
        this.sender = sender;
        this.requestManager = requestManager;
        this.retryExecutor = retryExecutor;
        this.config = config;
    }

    @Override
    public ListenableFuture<Result> get(String table, Get get) {
        return simpleAction(table, GET_TARGET_METHOD, get, GET_RESPONSE_PARSER);
    }

    @Override
    public ListenableFuture<ImmutableList<Result>> multiGet(String table, final ImmutableList<Get> gets) {
        return multiActionRequest(table, gets);
    }

    @Override
    public ListenableFuture<Void> put(String table, Put put) {
        return simpleAction(table, PUT_TARGET_METHOD, put, PUT_RESPONSE_PARSER);
    }

    @Override
    public ListenableFuture<Void> multiPut(String table, ImmutableList<Put> puts) {
        return multiMutationRequest(table, puts);
    }

    @Override
    public ListenableFuture<Boolean> checkAndPut(String table, ColumnCheck check, Put put) {
        return checkedAction(table, CHECK_AND_PUT_TARGET_METHOD, check, put, CHECK_AND_PUT_RESPONSE_PARSER);
    }

    @Override
    public ListenableFuture<Void> delete(String table, Delete delete) {
        return simpleAction(table, DELETE_TARGET_METHOD, delete, DELETE_RESPONSE_PARSER);
    }

    @Override
    public ListenableFuture<Void> multiDelete(String table, ImmutableList<Delete> deletes) {
        return multiMutationRequest(table, deletes);
    }

    @Override
    public ListenableFuture<Boolean> checkAndDelete(String table, ColumnCheck check, Delete delete) {
        return checkedAction(table, CHECK_AND_DELETE_TARGET_METHOD, check, delete, CHECK_AND_DELETE_RESPONSE_PARSER);
    }

    @Override
    public ScannerResultStream getScannerStream(String table, final Scan scan) {
        ScanCoordinator controller = new ScanCoordinator(
                table,
                scan,
                sender,
                requestManager,
                retryExecutor,
                topology,
                config
        );

        return new ScannerResultStream(controller);
    }

    @Override
    public ListenableFuture<Long> incrementColumnValue(String table, final Column column, final long amount) {
        Function<Object, byte[]> rowExtractor = Functions.constant(column.getRow());

        Function<HRegionLocation, Invocation> invocationBuilder = new Function<HRegionLocation, Invocation>() {
            @Override
            public Invocation apply(HRegionLocation location) {
                return new Invocation(INCREMENT_COL_VALUE_TARGET_METHOD, TARGET_PROTOCOL, new Object[]{
                        location.getRegionInfo().getRegionName(),
                        column.getRow(),
                        column.getFamily(),
                        column.getQualifier(),
                        amount,
                        true
                });
            }
        };

        return singleActionRequest(table, column, rowExtractor, invocationBuilder, INCREMENT_COL_VALUE_RESPONSE_PARSER);
    }

    public <A extends Row> ListenableFuture<Boolean> checkedAction(String table,
                                                                   final Method targetMethod,
                                                                   final ColumnCheck check,
                                                                   final A action,
                                                                   Function<HbaseObjectWritable, Boolean> responseParser) {

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

        return singleRowRequest(table, action, invocationBuilder, responseParser);
    }

    public <A extends Row, R> ListenableFuture<R> singleRowRequest(String table,
                                                                   A action,
                                                                   Function<HRegionLocation, Invocation> invocationBuilder,
                                                                   Function<HbaseObjectWritable, R> responseParser) {

        return singleActionRequest(table, action, ROW_OPERATION_ROW_EXRACTOR, invocationBuilder, responseParser);
    }

    public <P, R> ListenableFuture<R> singleActionRequest(final String table,
                                                          final P param,
                                                          final Function<? super P, byte[]> rowExtractor,
                                                          final Function<HRegionLocation, Invocation> invocationBuilder,
                                                          Function<HbaseObjectWritable, R> responseParser) {
        final byte[] row = rowExtractor.apply(param);
        RequestDetailProvider detailProvider = new RequestDetailProvider() {
            @Override
            public HRegionLocation getLocation() {
                return topology.getRegionServer(table, row);
            }

            @Override
            public HRegionLocation getRetryLocation() {
                return topology.getRegionServerNoCache(table, row);
            }

            @Override
            public Invocation getInvocation(HRegionLocation targetLocation) {
                return invocationBuilder.apply(targetLocation);
            }
        };

        HbaseOperationResultFuture<R> future = new HbaseOperationResultFuture<R>(requestManager);
        SingleActionController.initiate(
                detailProvider,
                future,
                responseParser,
                sender,
                retryExecutor,
                config
        );

        return future;
    }

    private <A extends Row, R> ListenableFuture<R> simpleAction(String table,
                                                                final Method targetMethod,
                                                                final A action,
                                                                Function<HbaseObjectWritable, R> responseParser) {

        Function<HRegionLocation, Invocation> invocationBuilder = createLocationAndParamInvocationBuilder(targetMethod, action);
        return singleRowRequest(table, action, invocationBuilder, responseParser);
    }

    private <A> Function<HRegionLocation, Invocation> createLocationAndParamInvocationBuilder(final Method targetMethod,
                                                                                              final A action) {
        return new Function<HRegionLocation, Invocation>() {
            @Override
            public Invocation apply(HRegionLocation invocationLocation) {
                return new Invocation(targetMethod, TARGET_PROTOCOL, new Object[] {
                        invocationLocation.getRegionInfo().getRegionName(),
                        action
                });
            }
        };
    }

    private <A extends Row> ListenableFuture<ImmutableList<Result>> multiActionRequest(String table,
                                                                                       ImmutableList<A> actions) {

        HbaseOperationResultFuture<ImmutableList<Result>> future =
                new HbaseOperationResultFuture<ImmutableList<Result>>(requestManager);

        MultiActionCoordinator.initiate(table, actions, future, topology, sender, retryExecutor);

        return future;
    }

    private <M extends Row> ListenableFuture<Void> multiMutationRequest(String table, ImmutableList<M> mutations) {
        ListenableFuture<ImmutableList<Result>> results = multiActionRequest(table, mutations);
        return Futures.transform(results, Functions.<Void>constant(null));
    }

}
