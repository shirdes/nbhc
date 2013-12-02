package org.wizbang.hbase.nbhc.dispatch;

import com.google.common.net.HostAndPort;

public interface RegionServerDispatcher {

    void request(HostAndPort host, Request request);

}
