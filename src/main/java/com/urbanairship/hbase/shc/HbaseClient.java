package com.urbanairship.hbase.shc;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.*;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.urbanairship.hbase.shc.dispatch.HbaseOperationResultFuture;
import com.urbanairship.hbase.shc.dispatch.RegionServerDispatcher;
import com.urbanairship.hbase.shc.dispatch.RequestManager;
import com.urbanairship.hbase.shc.dispatch.ResultBroker;
import com.urbanairship.hbase.shc.response.MultiResponseParser;
import com.urbanairship.hbase.shc.response.RemoteError;
import com.urbanairship.hbase.shc.response.ResponseCallback;
import com.urbanairship.hbase.shc.response.VoidResponseParser;
import com.urbanairship.hbase.shc.scan.ScannerBatchResult;
import com.urbanairship.hbase.shc.scan.ScannerResultStream;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.*;
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

    private static final Function<Row, byte[]> ROW_OPERATION_ROW_EXRACTOR = new Function<Row, byte[]>() {
        @Override
        public byte[] apply(Row operation) {
            return operation.getRow();
        }
    };

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
        return simpleResultLocationAndParamRequest(table, get, ROW_OPERATION_ROW_EXRACTOR, GET_TARGET_METHOD, GET_RESPONSE_PARSER);
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

                    // TODO: if needRetry has stuff in it, then need to issue the request for those rows.

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
        return simpleResultLocationAndParamRequest(table, put, ROW_OPERATION_ROW_EXRACTOR, PUT_TARGET_METHOD, PUT_RESPONSE_PARSER);
    }

    public ListenableFuture<Void> multiPut(String table, List<Put> puts) {
        return makeMultiMutationRequest(table, puts, "put");
    }

    public ListenableFuture<Void> delete(String table, Delete delete) {
        return simpleResultLocationAndParamRequest(table, delete, ROW_OPERATION_ROW_EXRACTOR, DELETE_TARGET_METHOD, DELETE_RESPONSE_PARSER);
    }

    public ListenableFuture<Void> multiDelete(String table, List<Delete> deletes) {
        return makeMultiMutationRequest(table, deletes, "delete");
    }

    public ScannerResultStream getScannerStream(String table, Scan scan) {
        // TODO: ensure that the Scan object has a value set for caching?  Otherwise we'd have to have a default given
        // TODO: to this client as a data member?

        return null;
    }

    private ListenableFuture<Long> openScanner(String table, Scan scan) {
        Function<Scan, byte[]> rowExtractor = new Function<Scan, byte[]>() {
            @Override
            public byte[] apply(Scan scan) {
                return scan.getStartRow();
            }
        };

        return simpleResultLocationAndParamRequest(table, scan, rowExtractor, OPEN_SCANNER_TARGET_METHOD, OPEN_SCANNER_RESPONSE_PARSER);
    }

    private ListenableFuture<Void> closeScanner(HRegionLocation location, long scannerId) {
        Invocation invocation = new Invocation(CLOSE_SCANNER_TARGET_METHOD, TARGET_PROTOCOL, new Object[]{scannerId});
        Supplier<HRegionLocation> sameLocationSupplier = Suppliers.ofInstance(location);

        return simpleResultRequest(location, invocation, CLOSE_SCANNER_RESPONSE_PARSER, sameLocationSupplier);
    }

    // TODO: don't like having a method with two numbers right next to each other as params. Perhaps next batch identifier object or something?
    private ListenableFuture<ScannerBatchResult> getScannerNextBatch(HRegionLocation location,
                                                                     long scannerId,
                                                                     int numResults) {

        Invocation invocation = new Invocation(SCANNER_NEXT_TARGET_METHOD, TARGET_PROTOCOL, new Object[]{
                scannerId,
                numResults
        });

        Operation operation = new Operation(getHost(location), invocation);

        final HbaseOperationResultFuture<ScannerBatchResult> future = new HbaseOperationResultFuture<ScannerBatchResult>(requestManager);

        // TODO: should probably pull this into a top level class since it has a chunk of logic
        HbaseResponseHandler responseHandler = new HbaseResponseHandler() {
            @Override
            public void handle(HbaseObjectWritable value) {
                Object object = value.get();
                if (object == null) {
                    future.communicateResult(ScannerBatchResult.allFinished());
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
        };

        // TODO: incorporate retries if possible and then handle the cases where we might need to reopend the scanner
        HbaseRemoteErrorHandler errorHandler = new HbaseRemoteErrorHandler() {
            @Override
            public void handle(RemoteError error, int attempt) {
                future.communicateError(new RemoteException(error.getErrorClass(),
                        error.getErrorMessage().isPresent() ? error.getErrorMessage().get() : ""));
            }
        };

        sendRequest(operation, future, responseHandler, errorHandler, 1);

        return future;
    }

    private <P, R> ListenableFuture<R> simpleResultLocationAndParamRequest(final String table,
                                                                           final P param,
                                                                           final Function<? super P, byte[]> rowExtractor,
                                                                           Method targetMethod,
                                                                           Function<HbaseObjectWritable, R> responseParser) {

        HRegionLocation location = topology.getRegionServer(table, rowExtractor.apply(param));

        Invocation invocation = new Invocation(targetMethod, TARGET_PROTOCOL, new Object[]{
                location.getRegionInfo().getRegionName(),
                param
        });

        Supplier<HRegionLocation> updatedLocationSupplier = new Supplier<HRegionLocation>() {
            @Override
            public HRegionLocation get() {
                return topology.getRegionServerNoCache(table, rowExtractor.apply(param));
            }
        };

        return simpleResultRequest(location, invocation, responseParser, updatedLocationSupplier);
    }

    private <P, R> ListenableFuture<R> simpleResultRequest(HRegionLocation location,
                                                           Invocation invocation,
                                                           Function<HbaseObjectWritable, R> responseParser,
                                                           Supplier<HRegionLocation> updatedLocationSupplier) {

        HbaseOperationResultFuture<R> future = new HbaseOperationResultFuture<R>(requestManager);

        SimpleResponseParsingResponseHandler<R> responseHandler = new SimpleResponseParsingResponseHandler<R>(future, responseParser);

        HbaseRemoteErrorHandler errorHandler = getRetryWithUpdatedLocationErrorHandler(invocation,
                updatedLocationSupplier, future, responseHandler);

        Operation operation = new Operation(getHost(location), invocation);

        sendRequest(operation, future, responseHandler, errorHandler, 1);

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

    private HbaseRemoteErrorHandler getRetryWithUpdatedLocationErrorHandler(final Invocation invocation,
                                                                            final Supplier<HRegionLocation> updatedLocationSupplier,
                                                                            final ResultBroker<?> resultBroker,
                                                                            final HbaseResponseHandler responseHandler) {
        return new HbaseRemoteErrorHandler() {
            @Override
            public void handle(RemoteError error, int attempt) {
                if (isRetryableError(error) && attempt <= maxRetries) {
                    HRegionLocation updatedLocation = updatedLocationSupplier.get();

                    Operation retryOperation = new Operation(getHost(updatedLocation), invocation);
                    sendRequest(retryOperation, resultBroker, responseHandler, this, attempt + 1);
                }
                else {
                    // TODO; get smarter about the error handling
                    resultBroker.communicateError(new RemoteException(error.getErrorClass(),
                            error.getErrorMessage().isPresent() ? error.getErrorMessage().get() : ""));
                }
            }
        };
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
