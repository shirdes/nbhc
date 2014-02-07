package org.wizbang.hbase.nbhc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.request.scan.ScannerResultStream;
import org.wizbang.hbase.nbhc.topology.HbaseMetaService;
import org.wizbang.hbase.nbhc.topology.HbaseMetaServiceFactory;

public final class ClientStartupService extends AbstractIdleService implements HbaseClientService {

    private final RequestManager requestManager;
    private final RegionServerDispatcherService dispatcherService;
    private final HbaseClientConfiguration config;

    private SchedulerWithWorkersRetryExecutor retryExecutor;

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

        retryExecutor = new SchedulerWithWorkersRetryExecutor(config);
        retryExecutor.startAndWait();

        metaService = HbaseMetaServiceFactory.create(requestManager, sender, retryExecutor, config);
        metaService.startAndWait();

        client = new HbaseClientImpl(metaService.getTopology(), sender, requestManager, retryExecutor, config);
    }

    @Override
    protected void shutDown() throws Exception {
        // TODO: is this the correct shutdown order??
        metaService.stopAndWait();
        dispatcherService.stopAndWait();
        retryExecutor.stopAndWait();
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