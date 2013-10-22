package com.urbanairship.hbase.shc.dispatch;

import com.urbanairship.hbase.shc.Operation;
import com.urbanairship.hbase.shc.response.ResponseCallback;

public interface RegionServerDispatcher {

    int request(Operation operation, ResponseCallback callback);

}
