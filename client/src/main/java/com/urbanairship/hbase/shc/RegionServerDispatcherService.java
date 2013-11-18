package com.urbanairship.hbase.shc;

import com.google.common.util.concurrent.Service;
import com.urbanairship.hbase.shc.dispatch.RegionServerDispatcher;

public interface RegionServerDispatcherService extends Service {

    RegionServerDispatcher getDispatcher();

}
