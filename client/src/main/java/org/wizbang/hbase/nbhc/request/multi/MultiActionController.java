package org.wizbang.hbase.nbhc.request.multi;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.wizbang.hbase.nbhc.Protocol.MULTI_ACTION_TARGET_METHOD;
import static org.wizbang.hbase.nbhc.Protocol.TARGET_PROTOCOL;

public class MultiActionController<A extends Row> {

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

    private final Object resultGatheringLock = new Object();

    private final SortedMap<Integer, Result> gatheredResults = new TreeMap<Integer, Result>();
    private boolean gatheringComplete = false;

    public MultiActionController(String table,
                                 ImmutableList<A> actions,
                                 ResultBroker<ImmutableList<Result>> resultBroker,
                                 RegionOwnershipTopology topology,
                                 RequestSender sender) {
        this.table = table;
        this.actions = actions;
        this.resultBroker = resultBroker;
        this.topology = topology;
        this.sender = sender;
    }

    public void initiate() {
        sendActionRequests(actions, allowCachedLocationLookup);
    }

    void processResponseResult(ImmutableMap<Integer, Result> successfulResults,
                                      ImmutableSet<Integer> retryActionIndexes) {

        synchronized (resultGatheringLock) {
            gatheredResults.putAll(successfulResults);
        }

        if (!retryActionIndexes.isEmpty()) {
            ImmutableList.Builder<A> retryActions = ImmutableList.builder();
            for (Integer index : retryActionIndexes) {
                retryActions.add(actions.get(index));
            }

            sendActionRequests(retryActions.build(), forceUncachedLocationLookup);
            return;
        }

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

    void processUnrecoverableError(Throwable error) {
        resultBroker.communicateError(error);
    }

    private void sendActionRequests(ImmutableList<A> sendActions, Function<byte[], HRegionLocation> locationLookup) {
        Map<HRegionLocation, MultiAction<A>> locationActions = groupByLocation(sendActions, locationLookup);
        for (HRegionLocation location : locationActions.keySet()) {
            MultiAction<A> multiAction = locationActions.get(location);

            Invocation invocation = new Invocation(MULTI_ACTION_TARGET_METHOD, TARGET_PROTOCOL, new Object[] {multiAction});

            MultiActionResponseHandler<A> responseHandler = new MultiActionResponseHandler<A>(this);

            sender.sendRequest(location, invocation, responseHandler, 1);
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