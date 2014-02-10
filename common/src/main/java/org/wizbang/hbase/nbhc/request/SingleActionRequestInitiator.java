package org.wizbang.hbase.nbhc.request;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.HbaseOperationResultFuture;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;

public class SingleActionRequestInitiator {

    private final RequestSender sender;
    private final RetryExecutor retryExecutor;
    private final RequestManager requestManager;
    private final HbaseClientConfiguration config;

    public SingleActionRequestInitiator(RequestSender sender,
                                        RetryExecutor retryExecutor,
                                        RequestManager requestManager,
                                        HbaseClientConfiguration config) {
        this.sender = sender;
        this.retryExecutor = retryExecutor;
        this.requestManager = requestManager;
        this.config = config;
    }

    public <R> ListenableFuture<R> initiate(RequestDetailProvider requestDetailProvider,
                                            Function<HbaseObjectWritable, R> responseParser) {

        HbaseOperationResultFuture<R> future = new HbaseOperationResultFuture<R>();

        final SingleActionController<R> controller = SingleActionController.initiate(
                requestDetailProvider,
                future,
                responseParser,
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
