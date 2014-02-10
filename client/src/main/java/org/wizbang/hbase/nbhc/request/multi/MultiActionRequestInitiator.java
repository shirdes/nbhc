package org.wizbang.hbase.nbhc.request.multi;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.HbaseOperationResultFuture;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

public class MultiActionRequestInitiator {

    private final RequestSender sender;
    private final RetryExecutor retryExecutor;
    private final RequestManager requestManager;
    private final RegionOwnershipTopology topology;
    private final HbaseClientConfiguration config;

    public MultiActionRequestInitiator(RequestSender sender,
                                       RetryExecutor retryExecutor,
                                       RequestManager requestManager,
                                       RegionOwnershipTopology topology,
                                       HbaseClientConfiguration config) {
        this.sender = sender;
        this.retryExecutor = retryExecutor;
        this.requestManager = requestManager;
        this.topology = topology;
        this.config = config;
    }

    public <A extends Row> ListenableFuture<ImmutableList<Result>> initiate(String table,
                                                                            ImmutableList<A> actions) {
        HbaseOperationResultFuture<ImmutableList<Result>> future = new HbaseOperationResultFuture<ImmutableList<Result>>();

        final MultiActionController<A> controller = MultiActionController.initiate(
                table,
                actions,
                future,
                topology,
                sender,
                retryExecutor,
                requestManager,
                config
        );

        future.setCancelCallback(new Runnable() {
            @Override
            public void run() {
                controller.cancel();
            }
        });

        return future;
    }
}
