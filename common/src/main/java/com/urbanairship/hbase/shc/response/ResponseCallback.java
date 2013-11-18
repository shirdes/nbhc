package com.urbanairship.hbase.shc.response;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public interface ResponseCallback {

    void receiveResponse(HbaseObjectWritable value);

    void receiveRemoteError(RemoteError remoteError);

    void receiveLocalError(Throwable error);

}
