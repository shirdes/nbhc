package org.wizbang.hbase.nbhc.request;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.wizbang.hbase.nbhc.response.RemoteError;

public interface ResponseHandler {

    void handleResponse(HbaseObjectWritable received);

    void handleRemoteError(RemoteError error, int attempt);

    void handleLocalError(Throwable error, int attempt);

}
