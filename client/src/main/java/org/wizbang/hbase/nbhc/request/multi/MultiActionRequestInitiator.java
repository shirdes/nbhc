package org.wizbang.hbase.nbhc.request.multi;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.RemoteErrorUtil;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.HbaseOperationResultFuture;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.response.RequestResponseController;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.wizbang.hbase.nbhc.Protocol.MULTI_ACTION_TARGET_METHOD;
import static org.wizbang.hbase.nbhc.Protocol.TARGET_PROTOCOL;

public class MultiActionRequestInitiator {

    private final RequestSender sender;
    private final RetryExecutor retryExecutor;
    private final ExecutorService workerPool;
    private final RequestManager requestManager;
    private final RegionOwnershipTopology topology;
    private final MultiActionResponseParser responseParser;
    private final RemoteErrorUtil errorUtil;
    private final HbaseClientConfiguration config;

    public MultiActionRequestInitiator(RequestSender sender,
                                       ExecutorService workerPool,
                                       RetryExecutor retryExecutor,
                                       RequestManager requestManager,
                                       RegionOwnershipTopology topology,
                                       MultiActionResponseParser responseParser,
                                       RemoteErrorUtil errorUtil,
                                       HbaseClientConfiguration config) {
        this.sender = sender;
        this.workerPool = workerPool;
        this.retryExecutor = retryExecutor;
        this.requestManager = requestManager;
        this.topology = topology;
        this.responseParser = responseParser;
        this.errorUtil = errorUtil;
        this.config = config;
    }

    public <A extends Row> ListenableFuture<ImmutableList<Result>> initiate(String table,
                                                                            ImmutableList<A> actions) {
        HbaseOperationResultFuture<ImmutableList<Result>> future = new HbaseOperationResultFuture<ImmutableList<Result>>();

        final MultiActionController<A> controller = new MultiActionController<A>(
                table,
                actions,
                future
        );

        future.setCancelCallback(new Runnable() {
            @Override
            public void run() {
                controller.cancel();
            }
        });

        workerPool.submit(new Runnable() {
            @Override
            public void run() {
                controller.launch();
            }
        });

        return future;
    }

    private final class MultiActionController<A extends Row> implements RequestResponseController {

        private final Function<byte[], HRegionLocation> allowCachedLocationLookup = new Function<byte[], HRegionLocation>() {
            @Override
            public HRegionLocation apply(byte[] row) {
                return topology.getRegionServer(table, row);
            }
        };

        private final Function<byte[], HRegionLocation> forceUncachedLocationLookup = new Function<byte[], HRegionLocation>() {
            @Override
            public HRegionLocation apply(byte[] row) {
                return topology.getRegionServerNoCache(table, row);
            }
        };

        private final String table;
        private final ImmutableList<A> actions;
        private final ResultBroker<ImmutableList<Result>> resultBroker;

        private final Object resultGatheringLock = new Object();

        private final SortedMap<Integer, Result> gatheredResults = new TreeMap<Integer, Result>();
        private boolean gatheringComplete = false;

        private final ConcurrentMap<Integer, ActiveRequestDetail> activeRequests = new ConcurrentHashMap<Integer, ActiveRequestDetail>();
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        private final AtomicInteger locationFailures = new AtomicInteger(0);
        private final AtomicInteger unknownFailures = new AtomicInteger(0);

        private MultiActionController(String table,
                                      ImmutableList<A> actions,
                                      ResultBroker<ImmutableList<Result>> resultBroker) {
            this.table = table;
            this.actions = actions;
            this.resultBroker = resultBroker;
        }

        private void launch() {
            ContiguousSet<Integer> indexes = ContiguousSet.create(Range.closedOpen(0, actions.size()), DiscreteDomain.integers());
            sendActionRequests(indexes, allowCachedLocationLookup);
        }

        @Override
        public void receiveResponse(int requestId, HbaseObjectWritable value) {
            activeRequests.remove(requestId);
            if (abandonDueToCancel()) {
                return;
            }

            MultiActionResponse response = responseParser.apply(value);
            if (response.isErrorResponse()) {
                failure(response.getError());
            }
            else {
                processResponseResult(response.getResults(), response.getIndexesNeedingRetry());
            }
        }

        @Override
        public void receiveRemoteError(int requestId, RemoteError remoteError) {
            // TODO: should we be retrying here?
            // TODO: want to pass along the host the request went to in the error
            failure(errorUtil.constructRemoteException(remoteError));
        }

        @Override
        public void receiveLocalError(int requestId, Throwable error) {
            // TODO: implement, this should be retriable typically?
            ActiveRequestDetail requestDetail = activeRequests.get(requestId);
            // TODO: don't think it could be null but...

            // TODO: want to pass along the host that the request went to in the error
            handleLocalError(requestDetail.actionIndexes, error);
        }

        @Override
        public void receiveFatalError(int requestId, Throwable error) {
            // TODO: want to pass along the host the request went to in the error
            failure(error);
        }

        @Override
        public void cancel() {
            if (!cancelled.compareAndSet(false, true)) {
                return;
            }

            unregisterActiveRequests();
        }

        private void unregisterActiveRequests() {
            for (Integer requestId : activeRequests.keySet()) {
                requestManager.unregisterResponseCallback(requestId);
            }
        }

        private boolean abandonDueToCancel() {
            // TODO: metric
            return cancelled.get();
        }

        private void processResponseResult(ImmutableMap<Integer, Result> successfulResults,
                                           ImmutableSet<Integer> retryActionIndexes) {

            synchronized (resultGatheringLock) {
                gatheredResults.putAll(successfulResults);
            }

            // TODO: How do we check for the maximum number of retries for location errors and then fail when we reach it?
            if (!retryActionIndexes.isEmpty()) {
                retry(retryActionIndexes);
            }
            else {
                determineCompletionStatus();
            }
        }

        private void determineCompletionStatus() {
            Optional<ImmutableList<Result>> completedResult = Optional.absent();
            synchronized (resultGatheringLock) {
                if (!gatheringComplete && (actions.size() == gatheredResults.size())) {
                    gatheringComplete = true;
                    completedResult = Optional.of(ImmutableList.copyOf(gatheredResults.values()));
                }
            }

            if (completedResult.isPresent()) {
                resultBroker.communicateResult(completedResult.get());
            }
        }

        private void handleLocalError(ImmutableSet<Integer> associatedActionIndexes,
                                      Throwable error) {
            if (shouldRetryUnknownError()) {
                retry(associatedActionIndexes);
            }
            else {
                failure(error);
            }
        }

        private boolean shouldRetryUnknownError() {
            int errors = unknownFailures.incrementAndGet();
            return errors <= config.maxUnknownErrorRetries;
        }

        private void failure(Throwable error) {
            // TODO: metric
            unregisterActiveRequests();
            resultBroker.communicateError(error);
        }

        private void retry(final ImmutableSet<Integer> retryActionIndexes) {
            if (abandonDueToCancel()) {
                return;
            }

            Runnable retry = new Runnable() {
                @Override
                public void run() {
                    sendActionRequests(retryActionIndexes, forceUncachedLocationLookup);
                }
            };

            retryExecutor.retry(retry);
        }

        private void sendActionRequests(ImmutableSet<Integer> indexes,
                                        Function<byte[], HRegionLocation> locationLookup) {
            if (abandonDueToCancel()) {
                return;
            }

            Multimap<HostAndPort, LocationActionIndex> hostLocationActionIndexes;
            try {
                hostLocationActionIndexes = groupActionIndexesByHost(indexes, locationLookup);
            }
            catch (Exception e) {
                handleLocalError(indexes, e);
                return;
            }

            for (HostAndPort host : hostLocationActionIndexes.keySet()) {
                Collection<LocationActionIndex> actionIndexes = hostLocationActionIndexes.get(host);
                MultiAction<A> multiAction = createMultiAction(actionIndexes);

                indexes = FluentIterable.from(actionIndexes).transform(INDEX_EXTRACTOR).toSet();

                Invocation invocation = new Invocation(MULTI_ACTION_TARGET_METHOD, TARGET_PROTOCOL, new Object[] {multiAction});
                int requestId;
                try {
                    requestId = sender.sendRequest(host, invocation, this);
                }
                catch (Exception e) {
                    handleLocalError(indexes, e);
                    continue;
                }

                indexes = FluentIterable.from(actionIndexes).transform(INDEX_EXTRACTOR).toSet();
                activeRequests.put(requestId, new ActiveRequestDetail(host, indexes));
            }
        }

        private MultiAction<A> createMultiAction(Collection<LocationActionIndex> actionIndexes) {
            MultiAction<A> multi = new MultiAction<A>();
            for (LocationActionIndex index : actionIndexes) {
                A action = actions.get(index.actionIndex);
                multi.add(index.location.getRegionInfo().getRegionName(), new Action<A>(action, index.actionIndex));
            }

            return multi;
        }

        private Multimap<HostAndPort, LocationActionIndex> groupActionIndexesByHost(ImmutableSet<Integer> indexes,
                                                                                    Function<byte[], HRegionLocation> locationLookup) {
            Multimap<HostAndPort, LocationActionIndex> regionActionIndexes = LinkedHashMultimap.create();
            for (Integer index : indexes) {
                A operation = actions.get(index);

                HRegionLocation location = locationLookup.apply(operation.getRow());
                HostAndPort host = HostAndPort.fromParts(location.getHostname(), location.getPort());
                regionActionIndexes.put(host, new LocationActionIndex(location, index));
            }

            return regionActionIndexes;
        }
    }

    private static final Function<LocationActionIndex, Integer> INDEX_EXTRACTOR = new Function<LocationActionIndex, Integer>() {
        @Override
        public Integer apply(LocationActionIndex index) {
            return index.actionIndex;
        }
    };

    private static final class LocationActionIndex {

        private final HRegionLocation location;
        private final int actionIndex;

        private LocationActionIndex(HRegionLocation location, int actionIndex) {
            this.location = location;
            this.actionIndex = actionIndex;
        }
    }

    private static final class ActiveRequestDetail {

        private final HostAndPort host;
        private final ImmutableSet<Integer> actionIndexes;

        private ActiveRequestDetail(HostAndPort host, ImmutableSet<Integer> actionIndexes) {
            this.host = host;
            this.actionIndexes = actionIndexes;
        }
    }
}
