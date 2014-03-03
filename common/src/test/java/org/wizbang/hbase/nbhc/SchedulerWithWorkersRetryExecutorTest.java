package org.wizbang.hbase.nbhc;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class SchedulerWithWorkersRetryExecutorTest {

    private ExecutorService pool;
    private HbaseClientConfiguration config;

    @Before
    public void setUp() throws Exception {
        pool = mock(ExecutorService.class);
        config = new HbaseClientConfiguration();
    }

    @Test
    public void testRetryOperation() throws Exception {
        SchedulerWithWorkersRetryExecutor executor = new SchedulerWithWorkersRetryExecutor(pool, config);
        executor.startAndWait();

        Runnable operation = mock(Runnable.class);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicLong executionTimestamp = new AtomicLong();
        when(pool.submit(Matchers.<Runnable>any())).thenAnswer(new Answer<Future<?>>() {
            @Override
            public Future<?> answer(InvocationOnMock invocationOnMock) throws Throwable {
                executionTimestamp.set(System.currentTimeMillis());
                latch.countDown();
                return Futures.immediateFuture(null);
            }
        });

        long submitted = System.currentTimeMillis();
        executor.retry(operation);

        latch.await(10, TimeUnit.SECONDS);

        assertTrue(executionTimestamp.get() - submitted >= config.operationRetryDelayMillis);

        verify(pool).submit(operation);

        executor.stop();
    }

    @Test
    public void testWorkerPoolSubmissionFailureDoesNotTerminateService() throws Exception {
        SchedulerWithWorkersRetryExecutor executor = new SchedulerWithWorkersRetryExecutor(pool, config);
        executor.startAndWait();

        Runnable operation = mock(Runnable.class);

        final CountDownLatch latch = new CountDownLatch(1);
        when(pool.submit(Matchers.<Runnable>any()))
                .thenThrow(new RejectedExecutionException("nope"))
                .thenAnswer(new Answer<Future<?>>() {
                    @Override
                    public Future<?> answer(InvocationOnMock invocationOnMock) throws Throwable {
                        latch.countDown();
                        return Futures.immediateFuture(null);
                    }
                });

        // First one fails
        executor.retry(operation);

        // Second one will succeed
        Runnable operation2 = mock(Runnable.class);
        executor.retry(operation2);

        latch.await(10, TimeUnit.SECONDS);

        verify(pool).submit(operation);
        verify(pool).submit(operation2);

        executor.stopAndWait();
    }

    @Test
    public void testAwaitingTasksExecutingDuringShutdown() throws Exception {
        SchedulerWithWorkersRetryExecutor executor = new SchedulerWithWorkersRetryExecutor(pool, config);
        executor.startAndWait();

        final AtomicInteger count = new AtomicInteger(0);
        final Future<?> f = Futures.immediateFuture(null);
        when(pool.submit(Matchers.<Runnable>any())).thenAnswer(new Answer<Future<?>>() {
            @Override
            public Future<?> answer(InvocationOnMock invocationOnMock) throws Throwable {
                count.incrementAndGet();
                return f;
            }
        });

        Runnable operation = mock(Runnable.class);
        for (int i = 0; i < 50; i++) {
            executor.retry(operation);
        }

        ListenableFuture<Service.State> future = executor.stop();

        Service.State state = future.get(10, TimeUnit.SECONDS);
        assertEquals(Service.State.TERMINATED, state);

        assertEquals(50, count.get());
    }

    @Test
    public void testShutdownTakesTooLongForcefulShutdownInitiated() throws Exception {
        SchedulerWithWorkersRetryExecutor executor = new SchedulerWithWorkersRetryExecutor(pool, config);
        executor.startAndWait();

        final CountDownLatch latch = new CountDownLatch(1);
        when(pool.submit(Matchers.<Runnable>any())).thenAnswer(new Answer<Future<?>>() {
            @Override
            public Future<?> answer(InvocationOnMock invocationOnMock) throws Throwable {
                try {
                    latch.await(60, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    // Good, this means that the executor was forcefully shutdown since we got interrupted
                }
                return Futures.immediateFuture(null);
            }
        });

        executor.retry(mock(Runnable.class));

        ListenableFuture<Service.State> stop = executor.stop();
        Service.State state = stop.get(30, TimeUnit.SECONDS);

        assertEquals(Service.State.TERMINATED, state);
    }
}
