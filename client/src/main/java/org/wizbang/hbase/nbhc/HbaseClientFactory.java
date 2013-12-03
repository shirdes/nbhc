package org.wizbang.hbase.nbhc;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.netty.NettyDispatcherFactory;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.topology.HbaseMetaService;
import org.wizbang.hbase.nbhc.topology.HbaseMetaServiceFactory;

public final class HbaseClientFactory {

    public static HbaseClientService create() {
        RequestManager requestManager = new RequestManager();

        RegionServerDispatcherService dispatcherService = NettyDispatcherFactory.create(requestManager);

        return new ClientService(requestManager, dispatcherService);
    }

    private static final class ClientService extends AbstractIdleService implements HbaseClientService {

        private final RequestManager requestManager;
        private final RegionServerDispatcherService dispatcherService;

        private HbaseMetaService metaService;

        private HbaseClient client;

        private ClientService(RequestManager requestManager,
                              RegionServerDispatcherService dispatcherService) {
            this.requestManager = requestManager;
            this.dispatcherService = dispatcherService;
        }

        @Override
        protected void startUp() throws Exception {
            dispatcherService.startAndWait();

            RequestSender sender = new RequestSender(requestManager, dispatcherService.getDispatcher());

            metaService = HbaseMetaServiceFactory.create(requestManager, sender);
            metaService.startAndWait();

            client = new HbaseClient(metaService.getTopology(), sender, requestManager, 1);
        }

        @Override
        protected void shutDown() throws Exception {
            metaService.stopAndWait();
            dispatcherService.stopAndWait();
        }

        @Override
        public HbaseClient getClient() {
            Preconditions.checkState(state() == State.RUNNING);
            return client;
        }
    }
}
