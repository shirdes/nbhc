package com.urbanairship.hbase.shc.request;

import com.urbanairship.hbase.shc.dispatch.HbaseOperationResultFuture;
import com.urbanairship.hbase.shc.dispatch.RequestManager;

public class OperationFutureSupplier {

    private final RequestManager requestManager;

    public OperationFutureSupplier(RequestManager requestManager) {
        this.requestManager = requestManager;
    }

    public <R>HbaseOperationResultFuture<R> create() {
        return new HbaseOperationResultFuture<R>(requestManager);
    }

}
