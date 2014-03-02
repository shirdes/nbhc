package org.wizbang.hbase.nbhc.dispatch;

import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.commons.lang.math.RandomUtils.nextInt;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class HbaseOperationResultFutureTest {

    @Test
    public void testResult() throws Exception {
        final HbaseOperationResultFuture<Integer> future = new HbaseOperationResultFuture<Integer>();

        final int result = nextInt();
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            @Override
            public void run() {
                future.communicateResult(result);
            }
        });

        Integer received = future.get(10, TimeUnit.SECONDS);

        assertEquals(result, received.intValue());
    }

    @Test
    public void testExecutionException() throws Exception {
        final HbaseOperationResultFuture<Integer> future = new HbaseOperationResultFuture<Integer>();

        Executors.newSingleThreadExecutor().submit(new Runnable() {
            @Override
            public void run() {
                future.communicateError(new RuntimeException("Boom"));
            }
        });

        try {
            future.get(10, TimeUnit.SECONDS);
            fail();
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertTrue(e.getCause().getMessage().equals("Boom"));
        }
    }

    @Test
    public void testTimeoutCallback() throws Exception {
        HbaseOperationResultFuture<Integer> future = new HbaseOperationResultFuture<Integer>();

        final AtomicInteger calls = new AtomicInteger(0);
        future.setCancelCallback(new Runnable() {
            @Override
            public void run() {
                calls.incrementAndGet();
            }
        });

        try {
            future.get(1, TimeUnit.SECONDS);
            fail();
        }
        catch (TimeoutException e) {
            // All good
        }

        assertEquals(1, calls.get());
    }

    @Test
    public void testInterruptionHitsCallback() throws Exception {
        HbaseOperationResultFuture<Integer> future = new HbaseOperationResultFuture<Integer>();
        final AtomicInteger calls = new AtomicInteger(0);
        future.setCancelCallback(new Runnable() {
            @Override
            public void run() {
                calls.incrementAndGet();
            }
        });

        future.cancel(true);

        assertEquals(1, calls.get());
    }

    @Test
    public void testCallbackCanOnlyBeSetOnce() throws Exception {
        HbaseOperationResultFuture<Integer> future = new HbaseOperationResultFuture<Integer>();

        Runnable callback = mock(Runnable.class);
        future.setCancelCallback(callback);

        try {
            future.setCancelCallback(callback);
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("already been set"));
        }
    }
}
