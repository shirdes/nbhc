package org.wizbang.hbase.nbhc.topology;

import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.request.SingleActionRequestInitiator;

public final class HbaseMetaServiceFactory {

    public static HbaseMetaService create(SingleActionRequestInitiator singleActionRequestInitiator,
                                          HbaseClientConfiguration config) {
        return new MetaStartupService(singleActionRequestInitiator, config);
    }
}
