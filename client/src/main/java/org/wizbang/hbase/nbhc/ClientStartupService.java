package org.wizbang.hbase.nbhc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.LogManager;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.request.SingleActionRequestInitiator;
import org.wizbang.hbase.nbhc.request.multi.MultiActionRequestInitiator;
import org.wizbang.hbase.nbhc.request.multi.MultiActionResponseParser;
import org.wizbang.hbase.nbhc.request.scan.ScannerInitiator;
import org.wizbang.hbase.nbhc.request.scan.ScannerResultStream;
import org.wizbang.hbase.nbhc.topology.HbaseMetaService;
import org.wizbang.hbase.nbhc.topology.HbaseMetaServiceFactory;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class ClientStartupService extends AbstractIdleService implements HbaseClientService {

    private final RequestManager requestManager;
    private final RegionServerDispatcherService dispatcherService;
    private final HbaseClientConfiguration config;

    private SchedulerWithWorkersRetryExecutor retryExecutor;
    private ExecutorService workerPool;

    private HbaseMetaService metaService;

    private HbaseClient client;

    public ClientStartupService(RequestManager requestManager,
                                RegionServerDispatcherService dispatcherService,
                                HbaseClientConfiguration config) {
        this.requestManager = requestManager;
        this.dispatcherService = dispatcherService;
        this.config = config;
    }

    @Override
    protected void startUp() throws Exception {
        dispatcherService.startAndWait();

        RequestSender sender = new RequestSender(requestManager, dispatcherService.getDispatcher());

        workerPool = getWorkerPool();

        retryExecutor = new SchedulerWithWorkersRetryExecutor(workerPool, config);
        retryExecutor.startAndWait();

        SingleActionRequestInitiator singleActionRequestInitiator = new SingleActionRequestInitiator(sender,
                retryExecutor, requestManager, workerPool, config);

        metaService = HbaseMetaServiceFactory.create(singleActionRequestInitiator);
        metaService.startAndWait();

        RegionOwnershipTopology topology = metaService.getTopology();

        MultiActionRequestInitiator multiActionRequestInitiator = new MultiActionRequestInitiator(sender, retryExecutor,
                requestManager, topology, MultiActionResponseParser.INSTANCE);

        ScannerInitiator scannerInitiator = new ScannerInitiator(topology, singleActionRequestInitiator, config);

        client = new HbaseClientImpl(topology, singleActionRequestInitiator, multiActionRequestInitiator, scannerInitiator);
    }

    private ExecutorService getWorkerPool() {
        return Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("Hbase Client Worker Pool Thread %d")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        LogManager.getLogger("HbaseClient").fatal("Unhandled error caught from thread " + t.getName(), e);
                    }
                })
                .build()
        );
    }

    @Override
    protected void shutDown() throws Exception {
        // TODO: is this the correct shutdown order??
        metaService.stopAndWait();
        dispatcherService.stopAndWait();
        retryExecutor.stopAndWait();

        // TODO: should be cleaner
        workerPool.shutdown();
    }

    @Override
    public HbaseClient getClient() {
        return new StateCheckingHbaseClient();
    }

    private HbaseClient delegate() {
        Preconditions.checkState(state() == State.RUNNING);
        return client;
    }

    private final class StateCheckingHbaseClient implements HbaseClient {

        @Override
        public ListenableFuture<Result> get(String table, Get get) {
            return delegate().get(table, get);
        }

        @Override
        public ListenableFuture<ImmutableList<Result>> multiGet(String table, ImmutableList<Get> gets) {
            return delegate().multiGet(table, gets);
        }

        @Override
        public ListenableFuture<Void> put(String table, Put put) {
            return delegate().put(table, put);
        }

        @Override
        public ListenableFuture<Void> multiPut(String table, ImmutableList<Put> puts) {
            return delegate().multiPut(table, puts);
        }

        @Override
        public ListenableFuture<Boolean> checkAndPut(String table, ColumnCheck check, Put put) {
            return delegate().checkAndPut(table, check, put);
        }

        @Override
        public ListenableFuture<Void> delete(String table, Delete delete) {
            return delegate().delete(table, delete);
        }

        @Override
        public ListenableFuture<Void> multiDelete(String table, ImmutableList<Delete> deletes) {
            return delegate().multiDelete(table, deletes);
        }

        @Override
        public ListenableFuture<Boolean> checkAndDelete(String table, ColumnCheck check, Delete delete) {
            return delegate().checkAndDelete(table, check, delete);
        }

        @Override
        public ScannerResultStream getScannerStream(String table, Scan scan) {
            return delegate().getScannerStream(table, scan);
        }

        @Override
        public ListenableFuture<Long> incrementColumnValue(String table, Column column, long amount) {
            return delegate().incrementColumnValue(table, column, amount);
        }
    }
}
