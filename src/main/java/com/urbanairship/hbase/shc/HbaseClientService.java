package com.urbanairship.hbase.shc;

import com.google.common.util.concurrent.Service;

public interface HbaseClientService extends Service {

    HbaseClient getClient();

}
