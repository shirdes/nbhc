package org.wizbang.hbase.nbhc;

public final class HbaseClientConfiguration {

    public final int maxRemoteErrorRetries = 3;

    public final long operationRetryDelayMillis = 1000L;

    public long openScannerTimeoutMillis = 30000L;

    public long retrieveScannerBatchTimeoutMillis = 30000L;

    public final int scannerBatchSize = 1000;
}
