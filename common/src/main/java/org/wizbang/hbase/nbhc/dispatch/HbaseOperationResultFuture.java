package org.wizbang.hbase.nbhc.dispatch;

import com.google.common.util.concurrent.AbstractFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public final class HbaseOperationResultFuture<R> extends AbstractFuture<R> implements ResultBroker<R> {

    private final AtomicReference<Runnable> cancelCallback = new AtomicReference<Runnable>(null);

    @Override
    public void communicateResult(R result) {
        set(result);
    }

    @Override
    public void communicateError(Throwable error) {
        setException(error);
    }

    public void setCancelCallback(Runnable callback) {
        boolean success = cancelCallback.compareAndSet(null, callback);
        if (!success) {
            throw new RuntimeException("Cancel callback has already been set! What are you trying to do?");
        }
    }

    @Override
    public R get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
        try {
            return super.get(timeout, unit);
        }
        catch (TimeoutException e) {
            issueCancelCallback();
            throw e;
        }
    }

    @Override
    protected void interruptTask() {
        issueCancelCallback();
    }

    private void issueCancelCallback() {
        Runnable callback = cancelCallback.get();
        if (callback != null) {
            callback.run();
        }
    }
}
