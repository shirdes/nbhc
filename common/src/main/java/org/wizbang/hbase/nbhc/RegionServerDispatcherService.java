package org.wizbang.hbase.nbhc;

import com.google.common.util.concurrent.Service;
import org.wizbang.hbase.nbhc.dispatch.RegionServerDispatcher;

public interface RegionServerDispatcherService extends Service {

    RegionServerDispatcher getDispatcher();

}
