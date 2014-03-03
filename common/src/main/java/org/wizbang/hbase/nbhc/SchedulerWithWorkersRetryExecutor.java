package org.wizbang.hbase.nbhc;

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

public final class SchedulerWithWorkersRetryExecutor extends AbstractIdleService implements RetryExecutor {

    private static final Logger log = LogManager.getLogger(SchedulerWithWorkersRetryExecutor.class);

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

        Runnable task = new Runnable() {
            @Override
            public void run() {
                workerPool.submit(operation);
            }
        };

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
        // TODO: graceful shutdown
        retryScheduler.shutdown();
    }
}
