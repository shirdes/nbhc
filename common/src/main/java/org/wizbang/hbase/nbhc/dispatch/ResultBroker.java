package org.wizbang.hbase.nbhc.dispatch;

public interface ResultBroker<R> {

    void communicateResult(R result);

    void communicateError(Throwable error);

}
