package com.urbanairship.hbase.shc.dispatch;

import com.google.common.util.concurrent.ListenableFuture;
import com.urbanairship.hbase.shc.operation.Operation;

public interface RegionServerDispatcher {

    <R> ListenableFuture<R> request(Operation<?, R> operation);

}
