package org.wizbang.hbase.nbhc;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
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
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.request.DefaultResponseHandler;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.request.ResponseProcessor;
import org.wizbang.hbase.nbhc.request.SimpleParseResponseProcessor;
import org.wizbang.hbase.nbhc.request.multi.MultiActionController;
import org.wizbang.hbase.nbhc.request.scan.ScanCloser;
import org.wizbang.hbase.nbhc.request.scan.ScanController;
import org.wizbang.hbase.nbhc.request.scan.ScanOpener;
import org.wizbang.hbase.nbhc.request.scan.ScanOperationConfig;
import org.wizbang.hbase.nbhc.request.scan.ScanResultsLoader;
import org.wizbang.hbase.nbhc.request.scan.ScannerBatchResult;
import org.wizbang.hbase.nbhc.request.scan.ScannerNextBatchResponseHandler;
import org.wizbang.hbase.nbhc.request.scan.ScannerOpenResult;
import org.wizbang.hbase.nbhc.request.scan.ScannerResultStream;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

import java.lang.reflect.Method;

import static org.wizbang.hbase.nbhc.Protocol.*;

public class HbaseClient {

    public static final Function<Row, byte[]> ROW_OPERATION_ROW_EXRACTOR = new Function<Row, byte[]>() {
        @Override
        public byte[] apply(Row operation) {
            return operation.getRow();
        }
    };

    private final RegionOwnershipTopology topology;
    private final RequestSender sender;
    private final RequestManager requestManager;
    private final int maxRetries;

    public HbaseClient(RegionOwnershipTopology topology,
                       RequestSender sender,
                       RequestManager requestManager,
                       int maxRetries) {
        this.topology = topology;
        this.sender = sender;
        this.requestManager = requestManager;
        this.maxRetries = maxRetries;
    }

    public ListenableFuture<Result> get(String table, Get get) {
        return simpleAction(table, GET_TARGET_METHOD, get, GET_RESPONSE_PARSER);
    }

    public ListenableFuture<ImmutableList<Result>> multiGet(String table, final ImmutableList<Get> gets) {
        return multiActionRequest(table, gets);
    }

    public ListenableFuture<Void> put(String table, Put put) {
        return simpleAction(table, PUT_TARGET_METHOD, put, PUT_RESPONSE_PARSER);
    }

    public ListenableFuture<Void> multiPut(String table, ImmutableList<Put> puts) {
        return multiMutationRequest(table, puts);
    }

    public ListenableFuture<Boolean> checkAndPut(String table, ColumnCheck check, Put put) {
        return checkedAction(table, CHECK_AND_PUT_TARGET_METHOD, check, put, CHECK_AND_PUT_RESPONSE_PARSER, maxRetries);
    }

    public ListenableFuture<Void> delete(String table, Delete delete) {
        return simpleAction(table, DELETE_TARGET_METHOD, delete, DELETE_RESPONSE_PARSER);
    }

    public ListenableFuture<Void> multiDelete(String table, ImmutableList<Delete> deletes) {
        return multiMutationRequest(table, deletes);
    }

    public ListenableFuture<Boolean> checkAndDelete(String table, ColumnCheck check, Delete delete) {
        return checkedAction(table, CHECK_AND_DELETE_TARGET_METHOD, check, delete, CHECK_AND_DELETE_RESPONSE_PARSER, maxRetries);
    }

    public ScannerResultStream getScannerStream(String table, final Scan scan) {
        // TODO: ensure that the Scan object has a value set for caching?  Otherwise we'd have to have a default given
        // TODO: to this client as a data member?
        // TODO: need to get the timeouts from somewhere
        ScanOperationConfig config = ScanOperationConfig.newBuilder()
                .build();

        ScanController controller = new ScanController(
                table, scan,
                new ScanOpener() {
                    @Override
                    public ListenableFuture<ScannerOpenResult> open(String table, Scan scan) {
                        return openScanner(table, scan);
                    }
                },
                new ScanResultsLoader() {
                    @Override
                    public ListenableFuture<ScannerBatchResult> load(HRegionLocation location, long scannerId) {
                        return getScannerNextBatch(location, scannerId, scan.getCaching());
                    }
                },
                new ScanCloser() {
                    @Override
                    public ListenableFuture<Void> close(HRegionLocation location, long scannerId) {
                        return closeScanner(location, scannerId);
                    }
                },
                config
        );

        return new ScannerResultStream(controller);
    }

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

        // TODO: this should be a singleton
        ResponseProcessor<Long> responseProcessor = new ResponseProcessor<Long>() {
            @Override
            public void process(HRegionLocation location, HbaseObjectWritable received, ResultBroker<Long> resultBroker) {
                resultBroker.communicateResult(INCREMENT_COL_VALUE_RESPONSE_PARSER.apply(received));
            }
        };

        return singleActionRequest(table, column, rowExtractor, invocationBuilder, responseProcessor, maxRetries);
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
        DefaultResponseHandler<R> responseHandler = new DefaultResponseHandler<R>(
                location,
                future,
                invocationBuilder,
                responseProcessor,
                updatedLocationSupplier,
                sender,
                maxRetries
        );

        sender.sendRequestForBroker(location, invocation, future, responseHandler, 1);

        return future;
    }

    private <A extends Row, R> ListenableFuture<R> simpleAction(String table,
                                                                final Method targetMethod,
                                                                final A action,
                                                                Function<HbaseObjectWritable, R> responseParser) {

        Function<HRegionLocation, Invocation> invocationBuilder = createLocationAndParamInvocationBuilder(targetMethod, action);
        return singleRowRequest(table, action, invocationBuilder, responseParser, maxRetries);
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

    private ListenableFuture<ScannerOpenResult> openScanner(final String table, final Scan scan) {
        Function<Object, byte[]> rowExtractor = Functions.constant(scan.getStartRow());

        // TODO: this can be static
        ResponseProcessor<ScannerOpenResult> responseProcessor = new ResponseProcessor<ScannerOpenResult>() {
            @Override
            public void process(HRegionLocation location, HbaseObjectWritable received, ResultBroker<ScannerOpenResult> resultBroker) {
                Long scannerId = OPEN_SCANNER_RESPONSE_PARSER.apply(received);
                resultBroker.communicateResult(new ScannerOpenResult(location, scannerId));
            }
        };

        Function<HRegionLocation, Invocation> invocationBuilder =
                createLocationAndParamInvocationBuilder(OPEN_SCANNER_TARGET_METHOD, scan);

        return singleActionRequest(table, scan, rowExtractor, invocationBuilder, responseProcessor, maxRetries);
    }

    private ListenableFuture<Void> closeScanner(HRegionLocation location, long scannerId) {
        final Invocation invocation = new Invocation(CLOSE_SCANNER_TARGET_METHOD, TARGET_PROTOCOL, new Object[]{scannerId});
        Function<HRegionLocation, Invocation> invocationBuilder = new Function<HRegionLocation, Invocation>() {
            @Override
            public Invocation apply(HRegionLocation input) {
                return invocation;
            }
        };

        Supplier<HRegionLocation> sameLocationProvider = Suppliers.ofInstance(location);

        SimpleParseResponseProcessor<Void> responseProcessor = new SimpleParseResponseProcessor<Void>(CLOSE_SCANNER_RESPONSE_PARSER);

        HbaseOperationResultFuture<Void> future = new HbaseOperationResultFuture<Void>(requestManager);
        DefaultResponseHandler<Void> responseHandler = new DefaultResponseHandler<Void>(
                location,
                future,
                invocationBuilder,
                responseProcessor,
                sameLocationProvider,
                sender,
                maxRetries
        );

        sender.sendRequestForBroker(location, invocation, future, responseHandler, 1);

        return future;
    }

    // TODO: don't like having a method with two numbers right next to each other as params. Perhaps next batch identifier object or something?
    private ListenableFuture<ScannerBatchResult> getScannerNextBatch(final HRegionLocation location,
                                                                     long scannerId,
                                                                     int numResults) {

        Invocation invocation = new Invocation(SCANNER_NEXT_TARGET_METHOD, TARGET_PROTOCOL, new Object[]{
                scannerId,
                numResults
        });

        HbaseOperationResultFuture<ScannerBatchResult> future = new HbaseOperationResultFuture<ScannerBatchResult>(requestManager);

        ScannerNextBatchResponseHandler responseHandler = new ScannerNextBatchResponseHandler(future);

        // TODO: what happens if the server says this region is not online or something and we should go back and find
        // TODO: the updated region to issue the request to?  Somehow that will need to bubble all the way out to the
        // TODO: state holder but don't want to tie this directly into that either :(
        sender.sendRequestForBroker(location, invocation, future, responseHandler, 1);

        return future;
    }

    private <A extends Row> ListenableFuture<ImmutableList<Result>> multiActionRequest(String table,
                                                                                       ImmutableList<A> actions) {

        HbaseOperationResultFuture<ImmutableList<Result>> future =
                new HbaseOperationResultFuture<ImmutableList<Result>>(requestManager);

        MultiActionController.initiate(table, actions, future, topology, sender);

        return future;
    }

    private <M extends Row> ListenableFuture<Void> multiMutationRequest(String table, ImmutableList<M> mutations) {
        ListenableFuture<ImmutableList<Result>> results = multiActionRequest(table, mutations);
        return Futures.transform(results, Functions.<Void>constant(null));
    }

}
