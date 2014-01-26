package org.wizbang.hbase.nbhc;

public interface RetryExecutor {
    // TODO: this should probably be on an interface so we don't expose the fact that this is a Service to users of it
    void retry(Runnable operation);
}
