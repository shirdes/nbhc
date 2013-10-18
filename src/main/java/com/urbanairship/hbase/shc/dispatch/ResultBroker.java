package com.urbanairship.hbase.shc.dispatch;

public interface ResultBroker<R> {

    void communicateResult(R result);

    void communicateError(Throwable error);

    void setCurrentActiveRequestId(int activeRequestId);

}
