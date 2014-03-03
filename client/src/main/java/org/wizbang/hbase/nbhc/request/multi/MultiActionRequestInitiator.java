package org.wizbang.hbase.nbhc.request.multi;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.ipc.RemoteException;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.HbaseOperationResultFuture;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.response.RequestResponseController;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.wizbang.hbase.nbhc.Protocol.MULTI_ACTION_TARGET_METHOD;
import static org.wizbang.hbase.nbhc.Protocol.TARGET_PROTOCOL;

public class MultiActionRequestInitiator {

    private final RequestSender sender;
    private final RetryExecutor retryExecutor;
    private final RequestManager requestManager;
    private final RegionOwnershipTopology topology;
    private final MultiActionResponseParser responseParser;

    public MultiActionRequestInitiator(RequestSender sender,
                                       RetryExecutor retryExecutor,
                                       RequestManager requestManager,
                                       RegionOwnershipTopology topology,
                                       MultiActionResponseParser responseParser) {
        this.sender = sender;
        this.retryExecutor = retryExecutor;
        this.requestManager = requestManager;
        this.topology = topology;
        this.responseParser = responseParser;
    }

    public <A extends Row> ListenableFuture<ImmutableList<Result>> initiate(String table,
                                                                            ImmutableList<A> actions) {
        HbaseOperationResultFuture<ImmutableList<Result>> future = new HbaseOperationResultFuture<ImmutableList<Result>>();

        final MultiActionController<A> controller = new MultiActionController<A>(
                table,
                actions,
                future,
                topology,
                sender,
                responseParser,
                retryExecutor,
                requestManager
        );

        future.setCancelCallback(new Runnable() {
            @Override
            public void run() {
                controller.cancel();
            }
        });

        controller.launch();

        return future;
    }

    private static final class MultiActionController<A extends Row> implements RequestResponseController {

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
        private final RegionOwnershipTopology topology;
        private final RequestSender sender;
        private final Function<HbaseObjectWritable, MultiActionResponse> parser;
        private final RetryExecutor retryExecutor;
        private final RequestManager requestManager;

        private final Object resultGatheringLock = new Object();

        private final SortedMap<Integer, Result> gatheredResults = new TreeMap<Integer, Result>();
        private boolean gatheringComplete = false;

        private final Set<Integer> activeRequestIds = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        private MultiActionController(String table,
                                      ImmutableList<A> actions,
                                      ResultBroker<ImmutableList<Result>> resultBroker,
                                      RegionOwnershipTopology topology,
                                      RequestSender sender,
                                      Function<HbaseObjectWritable, MultiActionResponse> parser,
                                      RetryExecutor retryExecutor,
                                      RequestManager requestManager) {
            this.table = table;
            this.actions = actions;
            this.resultBroker = resultBroker;
            this.topology = topology;
            this.sender = sender;
            this.parser = parser;
            this.retryExecutor = retryExecutor;
            this.requestManager = requestManager;
        }

        private void launch() {
            sendActionRequests(actions, allowCachedLocationLookup);
        }

        @Override
        public void receiveResponse(int requestId, HbaseObjectWritable value) {
            activeRequestIds.remove(requestId);
            if (cancelled.get()) {
                return;
            }

            MultiActionResponse response = parser.apply(value);
            if (response.isErrorResponse()) {
                // TODO: cancel remaining requests...
                resultBroker.communicateError(response.getError());
            }
            else {
                processResponseResult(response.getResults(), response.getIndexesNeedingRetry());
            }
        }

        @Override
        public void receiveRemoteError(int requestId, RemoteError remoteError) {
            activeRequestIds.remove(requestId);
            // TODO: cancel remaining requests
            resultBroker.communicateError(new RemoteException(remoteError.getErrorClass(),
                    (remoteError.getErrorMessage().isPresent() ? remoteError.getErrorMessage().get() : "")));
        }

        @Override
        public void receiveLocalError(int requestId, Throwable error) {
            // TODO: implement, this should be retriable typically?
        }

        @Override
        public void receiveFatalError(int requestId, Throwable error) {
            activeRequestIds.remove(requestId);
            // TODO: cancel remaining requests
            resultBroker.communicateError(error);
        }

        @Override
        public void cancel() {
            if (!cancelled.compareAndSet(false, true)) {
                return;
            }

            for (Integer requestId : activeRequestIds) {
                requestManager.unregisterResponseCallback(requestId);
            }
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

        private void retry(ImmutableSet<Integer> retryActionIndexes) {
            if (cancelled.get()) {
                return;
            }

            // TODO: maximum number of retries allowed??
            ImmutableList.Builder<A> builder = ImmutableList.builder();
            for (Integer index : retryActionIndexes) {
                builder.add(actions.get(index));
            }

            final ImmutableList<A> retryActions = builder.build();
            Runnable retry = new Runnable() {
                @Override
                public void run() {
                    // TODO: need try catch here to communicate error if it happens.
                    sendActionRequests(retryActions, forceUncachedLocationLookup);
                }
            };

            retryExecutor.retry(retry);
        }

        private void sendActionRequests(ImmutableList<A> sendActions, Function<byte[], HRegionLocation> locationLookup) {
            Map<HRegionLocation, MultiAction<A>> locationActions = groupByLocation(sendActions, locationLookup);
            for (HRegionLocation location : locationActions.keySet()) {
                MultiAction<A> multiAction = locationActions.get(location);

                Invocation invocation = new Invocation(MULTI_ACTION_TARGET_METHOD, TARGET_PROTOCOL, new Object[] {multiAction});
                int requestId = sender.sendRequest(location, invocation, this);
                activeRequestIds.add(requestId);
            }
        }

        private Map<HRegionLocation, MultiAction<A>> groupByLocation(ImmutableList<A> actions,
                                                                     Function<byte[], HRegionLocation> locationLookup) {
            Map<HRegionLocation, MultiAction<A>> regionActions = Maps.newHashMap();
            for (int i = 0; i < actions.size(); i++) {
                A operation = actions.get(i);

                HRegionLocation location = locationLookup.apply(operation.getRow());
                MultiAction<A> regionAction = regionActions.get(location);
                if (regionAction == null) {
                    regionAction = new MultiAction<A>();
                    regionActions.put(location, regionAction);
                }

                regionAction.add(location.getRegionInfo().getRegionName(), new Action<A>(operation, i));
            }

            return regionActions;
        }

    }
}