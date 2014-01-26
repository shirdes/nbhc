package org.wizbang.hbase.nbhc;

import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.netty.NettyDispatcherFactory;

public final class HbaseClientFactory {

    public static HbaseClientService create(HbaseClientConfiguration config) {
        RequestManager requestManager = new RequestManager();

        RegionServerDispatcherService dispatcherService = NettyDispatcherFactory.create(requestManager);

        return new ClientStartupService(requestManager, dispatcherService, config);
    }
}
