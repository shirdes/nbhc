package org.wizbang.hbase.nbhc.topology;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.request.OperationFutureSupplier;
import org.wizbang.hbase.nbhc.request.RequestSender;

public final class HbaseMetaServiceFactory {

    public static HbaseMetaService create(RequestManager requestManager, RequestSender sender) {
        return new Service(requestManager, sender);
    }

    private static final class Service extends AbstractIdleService implements HbaseMetaService {

        private final RequestManager requestManager;
        private final RequestSender sender;

        private CuratorFramework curator;
        private ZookeeperHbaseClusterTopology clusterTopology;

        private MetaTable metaTable;

        private Service(RequestManager requestManager, RequestSender sender) {
            this.requestManager = requestManager;
            this.sender = sender;
        }

        @Override
        protected void startUp() throws Exception {
            OperationFutureSupplier futureSupplier = new OperationFutureSupplier(requestManager);

            TopologyOperationsClient operationsClient = new TopologyOperationsClient(sender, futureSupplier, 3);
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
        public MetaTable getTopology() {
            Preconditions.checkState(state() == State.RUNNING);
            return metaTable;
        }
    }
}
