package org.wizbang.hbase.nbhc.topology;

import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.request.RequestSender;

public final class HbaseMetaServiceFactory {

    public static HbaseMetaService create(RequestManager requestManager,
                                          RequestSender sender,
                                          RetryExecutor retryExecutor,
                                          HbaseClientConfiguration config) {
        return new MetaStartupService(requestManager, sender, retryExecutor, config);
    }
}
