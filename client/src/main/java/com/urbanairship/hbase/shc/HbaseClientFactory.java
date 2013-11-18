package com.urbanairship.hbase.shc;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.urbanairship.hbase.shc.dispatch.RequestManager;
import com.urbanairship.hbase.shc.request.RequestSender;
import com.urbanairship.hbase.shc.topology.HConnectionRegionOwnershipTopology;
import com.urbanairship.hbase.shc.topology.RegionOwnershipTopology;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public final class HbaseClientFactory {

    public static HbaseClientService create(Configuration hbaseConfig) {
        RequestManager requestManager = new RequestManager();

        RegionServerDispatcherService dispatcherService = NettyDispatcherFactory.create(requestManager);

        return new ClientService(hbaseConfig, requestManager, dispatcherService);
    }

    private static final class ClientService extends AbstractIdleService implements HbaseClientService {

        private final Configuration hbaseConfig;
        private final RequestManager requestManager;
        private final RegionServerDispatcherService dispatcherService;

        private HbaseClient client;

        private ClientService(Configuration hbaseConfig,
                              RequestManager requestManager,
                              RegionServerDispatcherService dispatcherService) {
            this.hbaseConfig = hbaseConfig;
            this.requestManager = requestManager;
            this.dispatcherService = dispatcherService;
        }

        @Override
        protected void startUp() throws Exception {
            dispatcherService.startAndWait();

            HConnection hconn;
            try {
                hconn = HConnectionManager.createConnection(hbaseConfig);
                hconn.getRegionLocation(HConstants.ROOT_TABLE_NAME, HConstants.EMPTY_BYTE_ARRAY, false);
            }
            catch (Exception e) {
                throw new RuntimeException("Error creating hbase connection", e);
            }

            RegionOwnershipTopology topology = new HConnectionRegionOwnershipTopology(hconn);

            RequestSender sender = new RequestSender(dispatcherService.getDispatcher(), topology, requestManager);

            client = new HbaseClient(topology, sender, requestManager, 1);
        }

        @Override
        protected void shutDown() throws Exception {
            dispatcherService.stopAndWait();
        }

        @Override
        public HbaseClient getClient() {
            Preconditions.checkState(state() == State.RUNNING);
            return client;
        }
    }
}
