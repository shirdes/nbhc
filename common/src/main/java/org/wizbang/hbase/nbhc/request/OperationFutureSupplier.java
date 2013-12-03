package org.wizbang.hbase.nbhc.request;

import org.wizbang.hbase.nbhc.dispatch.HbaseOperationResultFuture;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;

public class OperationFutureSupplier {

    private final RequestManager requestManager;

    public OperationFutureSupplier(RequestManager requestManager) {
        this.requestManager = requestManager;
    }

    public <R>HbaseOperationResultFuture<R> create() {
        return new HbaseOperationResultFuture<R>(requestManager);
    }

}
