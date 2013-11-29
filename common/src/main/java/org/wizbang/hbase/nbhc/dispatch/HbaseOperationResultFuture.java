package org.wizbang.hbase.nbhc.dispatch;

import com.google.common.util.concurrent.AbstractFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public final class HbaseOperationResultFuture<R> extends AbstractFuture<R> implements ResultBroker<R> {

    private final AtomicReference<Integer> currentActiveRequestId = new AtomicReference<Integer>(null);

    private final RequestManager requestManager;

    public HbaseOperationResultFuture(RequestManager requestManager) {
        this.requestManager = requestManager;
    }

    @Override
    public void communicateResult(R result) {
        set(result);
    }

    @Override
    public void communicateError(Throwable error) {
        setException(error);
    }

    @Override
    public void setCurrentActiveRequestId(int activeRequestId) {
        currentActiveRequestId.set(activeRequestId);
    }

    @Override
    public R get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
        try {
            return super.get(timeout, unit);
        }
        catch (TimeoutException e) {
            Integer requestId = currentActiveRequestId.get();
            if (requestId != null) {
                requestManager.unregisterResponseCallback(currentActiveRequestId.get());
            }

            throw e;
        }
    }
}