package com.urbanairship.hbase.shc.request;

import com.urbanairship.hbase.shc.response.RemoteError;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public interface RequestController {

    void handleResponse(HbaseObjectWritable received);

    void handleRemoteError(RemoteError error, int attempt);

    void handleLocalError(Throwable error, int attempt);

}
