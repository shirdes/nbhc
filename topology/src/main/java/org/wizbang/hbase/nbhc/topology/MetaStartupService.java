package org.wizbang.hbase.nbhc.topology;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.hbase.HRegionLocation;
import org.wizbang.hbase.nbhc.request.SingleActionRequestInitiator;

public final class MetaStartupService extends AbstractIdleService implements HbaseMetaService {

    private final SingleActionRequestInitiator singleActionRequestInitiator;

    private CuratorFramework curator;
    private ZookeeperHbaseClusterTopology clusterTopology;

    private MetaTable metaTable;

    public MetaStartupService(SingleActionRequestInitiator singleActionRequestInitiator) {
        this.singleActionRequestInitiator = singleActionRequestInitiator;
    }

    @Override
    protected void startUp() throws Exception {
        TopologyOperationsClient operationsClient = new TopologyOperationsClient(singleActionRequestInitiator);
        MetaTableLookupSource metaSource = new MetaTableLookupSource(operationsClient, TopologyUtil.INSTACE);

        Supplier<LocationCache> cacheSupplier = new Supplier<LocationCache>() {
            @Override
            public LocationCache get() {
                return new LocationCache();
            }
        };

        // TODO: expose to config!!
        curator = CuratorFrameworkFactory.newClient("localhost:2181/hbase", new RetryNTimes(5, 100));
        curator.start();

        clusterTopology = new ZookeeperHbaseClusterTopology(curator);
        clusterTopology.startAndWait();

        RootTableLookupSource rootTableLookupSource = new RootTableLookupSource(clusterTopology, operationsClient, TopologyUtil.INSTACE);
        RootTable rootTable = new RootTable(rootTableLookupSource, new LocationCache());

        metaTable = new MetaTable(metaSource, cacheSupplier, rootTable);
    }

    @Override
    protected void shutDown() throws Exception {
        clusterTopology.stopAndWait();
        curator.close();
    }

    @Override
    public RegionOwnershipTopology getTopology() {
        return new StateCheckingMetaTable();
    }

    private RegionOwnershipTopology delegate() {
        Preconditions.checkState(state() == State.RUNNING);
        return metaTable;
    }

    private final class StateCheckingMetaTable implements RegionOwnershipTopology {

        @Override
        public HRegionLocation getRegionServer(String table, byte[] targetRow) {
            return delegate().getRegionServer(table, targetRow);
        }

        @Override
        public HRegionLocation getRegionServerNoCache(String table, byte[] targetRow) {
            return delegate().getRegionServerNoCache(table, targetRow);
        }
    }
}
