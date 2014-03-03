package org.wizbang.hbase.nbhc;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class SchedulerWithWorkersRetryExecutor extends AbstractIdleService implements RetryExecutor {

    private static final Logger log = LogManager.getLogger(SchedulerWithWorkersRetryExecutor.class);

    private static final Timer ACTUAL_RETRY_DELAY_TIMER = HbaseClientMetrics.timer("RetryExecutor:ActualDelay");
    private static final AtomicLong RETRY_QUEUE_SIZE = new AtomicLong(0L);
    static {
        HbaseClientMetrics.gauge("RetryExecutor:RetryQueueSize", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return RETRY_QUEUE_SIZE.get();
            }
        });
    }

    private final ExecutorService workerPool;
    private final HbaseClientConfiguration config;

    private ScheduledExecutorService retryScheduler;

    public SchedulerWithWorkersRetryExecutor(ExecutorService workerPool, HbaseClientConfiguration config) {
        this.workerPool = workerPool;
        this.config = config;
    }

    @Override
    public void retry(final Runnable operation) {
        Preconditions.checkState(state() == State.RUNNING);

        final long start = System.currentTimeMillis();
        Runnable task = new Runnable() {
            @Override
            public void run() {
                RETRY_QUEUE_SIZE.decrementAndGet();
                ACTUAL_RETRY_DELAY_TIMER.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                try {
                    workerPool.submit(operation);
                }
                catch (Throwable e) {
                    log.fatal("Failed to submit retry operation to worker pool! The retry is being dropped!", e);
                }
            }
        };

        RETRY_QUEUE_SIZE.incrementAndGet();
        retryScheduler.schedule(task, config.operationRetryDelayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void startUp() throws Exception {
        retryScheduler = Executors.newSingleThreadScheduledExecutor(getThreadFactory("Retry Scheduler"));
    }

    private ThreadFactory getThreadFactory(final String threadNamePrefix) {
        return new ThreadFactoryBuilder()
                .setNameFormat(threadNamePrefix + " %d")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        log.fatal("Uncaught exception from " + t.getName(), e);
                    }
                })
                .build();
    }

    @Override
    protected void shutDown() throws Exception {
        retryScheduler.shutdown();
        // At this point, any further submits for retry would fail because of state change of the service, so we should
        // really only need to wait the retry delay time for the pool to be done. Add a little bit more just to be sure.
        long waitMillis = config.operationRetryDelayMillis + 1000L;
        try {
            if (!retryScheduler.awaitTermination(waitMillis, TimeUnit.MILLISECONDS)) {
                log.error(String.format("Retry executor service did not cleanly shutdown in %d ms. Forcing shutdown!", waitMillis));
                retryScheduler.shutdownNow();
                if (!retryScheduler.awaitTermination(waitMillis, TimeUnit.MILLISECONDS)) {
                    log.fatal("Unable to force shutdown of retry executor service!");
                }
            }
        }
        catch (InterruptedException e) {
            log.error("Interrupted waiting for retry executor service to shutdown!");
            retryScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
