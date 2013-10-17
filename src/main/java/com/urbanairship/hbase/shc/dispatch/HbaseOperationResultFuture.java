package com.urbanairship.hbase.shc.dispatch;

import com.google.common.util.concurrent.AbstractFuture;

public final class HbaseOperationResultFuture<R> extends AbstractFuture<R> implements ResultBroker<R> {

    @Override
    public void communicateResult(R result) {
        set(result);
    }

    @Override
    public void communicateError(Throwable error) {
        setException(error);
    }
}
