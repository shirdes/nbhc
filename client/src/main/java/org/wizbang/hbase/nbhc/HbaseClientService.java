package org.wizbang.hbase.nbhc;

import com.google.common.util.concurrent.Service;

public interface HbaseClientService extends Service {

    HbaseClient getClient();

}
