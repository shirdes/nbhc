package org.wizbang.hbase.nbhc;

public interface RetryExecutor {

    void retry(Runnable operation);

}
