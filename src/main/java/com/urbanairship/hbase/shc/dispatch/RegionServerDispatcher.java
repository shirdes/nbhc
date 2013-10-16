package com.urbanairship.hbase.shc.dispatch;

import com.urbanairship.hbase.shc.operation.Operation;

public interface RegionServerDispatcher {

    int request(Operation operation, ResponseCallback callback);

}
