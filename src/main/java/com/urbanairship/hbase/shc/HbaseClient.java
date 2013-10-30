package com.urbanairship.hbase.shc;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.urbanairship.hbase.shc.dispatch.HbaseOperationResultFuture;
import com.urbanairship.hbase.shc.dispatch.RequestManager;
import com.urbanairship.hbase.shc.dispatch.ResultBroker;
import com.urbanairship.hbase.shc.request.*;
import com.urbanairship.hbase.shc.scan.*;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static com.urbanairship.hbase.shc.Protocol.*;

public class HbaseClient {

    private static final Function<Row, byte[]> ROW_OPERATION_ROW_EXRACTOR = new Function<Row, byte[]>() {
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

    public ListenableFuture<List<Result>> multiGet(String table, final List<Get> gets) {
        ListenableFuture<List<ImmutableMap<Integer, Result>>> all = multiActionRequest(table, gets);
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
        return simpleAction(table, PUT_TARGET_METHOD, put, PUT_RESPONSE_PARSER);
    }

    public ListenableFuture<Void> multiPut(String table, List<Put> puts) {
        return multiMutationRequest(table, puts);
    }

    public ListenableFuture<Boolean> checkAndPut(String table, ColumnCheck check, Put put) {
        return checkedAction(table, CHECK_AND_PUT_TARGET_METHOD, check, put, CHECK_AND_PUT_RESPONSE_PARSER);
    }

    public ListenableFuture<Void> delete(String table, Delete delete) {
        return simpleAction(table, DELETE_TARGET_METHOD, delete, DELETE_RESPONSE_PARSER);
    }

    public ListenableFuture<Void> multiDelete(String table, List<Delete> deletes) {
        return multiMutationRequest(table, deletes);
    }

    public ListenableFuture<Boolean> checkAndDelete(String table, ColumnCheck check, Delete delete) {
        return checkedAction(table, CHECK_AND_DELETE_TARGET_METHOD, check, delete, CHECK_AND_DELETE_RESPONSE_PARSER);
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

    private <A extends Row> ListenableFuture<Boolean> checkedAction(String table,
                                                                    final Method targetMethod,
                                                                    final ColumnCheck check,
                                                                    final A action,
                                                                    Function<HbaseObjectWritable, Boolean> responseParser) {

        Function<HRegionLocation, Invocation> invocationBuilder = new Function<HRegionLocation, Invocation>() {
            @Override
            public Invocation apply(HRegionLocation location) {
                return new Invocation(targetMethod, TARGET_PROTOCOL, new Object[] {
                        location.getRegionInfo().getRegionName(),
                        check.getRow(),
                        check.getFamily(),
                        check.getQualifier(),
                        (check.getValue().isPresent()) ? check.getValue().get() : null,
                        action
                });
            }
        };

        return singleRowRequest(table, action, invocationBuilder, responseParser);
    }

    private <A extends Row, R> ListenableFuture<R> singleRowRequest(String table,
                                                                    A action,
                                                                    Function<HRegionLocation, Invocation> invocationBuilder,
                                                                    Function<HbaseObjectWritable, R> responseParser) {

        SimpleParseResponseProcessor<R> processor = new SimpleParseResponseProcessor<R>(responseParser);
        return singleActionRequest(table, action, ROW_OPERATION_ROW_EXRACTOR, invocationBuilder, processor);
    }

    public ScannerResultStream getScannerStream(String table, final Scan scan) {
        // TODO: ensure that the Scan object has a value set for caching?  Otherwise we'd have to have a default given
        // TODO: to this client as a data member?
        // TODO: need to get the timeouts from somewhere
        ScanOperationConfig config = ScanOperationConfig.newBuilder()
                .build();

        ScanStateHolder stateHolder = new ScanStateHolder(
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

        return new ScannerResultStream(stateHolder);
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

        return singleActionRequest(table, scan, rowExtractor, invocationBuilder, responseProcessor);
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
        DefaultRequestController<Void> controller = new DefaultRequestController<Void>(
                location,
                future,
                invocationBuilder,
                responseProcessor,
                sameLocationProvider,
                sender,
                maxRetries
        );

        sender.sendRequest(location, invocation, future, controller, 1);

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

        ScannerNextBatchRequestController controller = new ScannerNextBatchRequestController(future);

        // TODO: what happens if the server says this region is not online or something and we should go back and find
        // TODO: the updated region to issue the request to?  Somehow that will need to bubble all the way out to the
        // TODO: state holder but don't want to tie this directly into that either :(
        sender.sendRequest(location, invocation, future, controller, 1);

        return future;
    }

    private <P, R> ListenableFuture<R> singleActionRequest(final String table,
                                                           final P param,
                                                           final Function<? super P, byte[]> rowExtractor,
                                                           Function<HRegionLocation, Invocation> invocationBuilder,
                                                           ResponseProcessor<R> responseProcessor) {

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
                sender,
                maxRetries
        );

        sender.sendRequest(location, invocation, future, controller, 1);

        return future;
    }

    private <A extends Row> ListenableFuture<List<ImmutableMap<Integer, Result>>> multiActionRequest(String table,
                                                                                                     final List<A> actions) {

        Map<HRegionLocation, MultiAction<A>> locationActions = groupByLocation(table, actions);

        Function<Integer, A> indexLookup = new Function<Integer, A>() {
            @Override
            public A apply(Integer index) {
                return actions.get(index);
            }
        };

        List<ListenableFuture<ImmutableMap<Integer, Result>>> futures = Lists.newArrayList();
        for (HRegionLocation location : locationActions.keySet()) {
            MultiAction<A> multiAction = locationActions.get(location);

            Invocation invocation = new Invocation(MULTI_ACTION_TARGET_METHOD, TARGET_PROTOCOL, new Object[] {multiAction});

            HbaseOperationResultFuture<ImmutableMap<Integer, Result>> future =
                    new HbaseOperationResultFuture<ImmutableMap<Integer, Result>>(requestManager);

            MultiActionRequestController<A> controller = new MultiActionRequestController<A>(indexLookup, future);

            sender.sendRequest(location, invocation, future, controller, 1);

            futures.add(future);
        }

        return Futures.allAsList(futures);
    }

    private <M extends Row> ListenableFuture<Void> multiMutationRequest(String table, List<M> mutations) {
        ListenableFuture<List<ImmutableMap<Integer, Result>>> futures = multiActionRequest(table, mutations);
        return Futures.transform(futures, Functions.<Void>constant(null));
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

}
