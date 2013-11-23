package org.wizbang.hbase.nbhc.dispatch;

import org.wizbang.hbase.nbhc.Operation;
import org.wizbang.hbase.nbhc.response.ResponseCallback;

public interface RegionServerDispatcher {

    int request(Operation operation, ResponseCallback callback);

}
