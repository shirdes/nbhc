package org.wizbang.hbase.nbhc;

public final class HbaseClientConfiguration {

    public final int maxLocationErrorRetries = 3;

    public final long operationRetryDelayMillis = 1000L;
}
