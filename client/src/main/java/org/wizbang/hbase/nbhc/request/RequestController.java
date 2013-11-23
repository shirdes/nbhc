package org.wizbang.hbase.nbhc.request;

import org.wizbang.hbase.nbhc.response.RemoteError;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public interface RequestController {

    void handleResponse(HbaseObjectWritable received);

    void handleRemoteError(RemoteError error, int attempt);

    void handleLocalError(Throwable error, int attempt);

}
