package com.urbanairship.hbase.shc.response;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public interface ResponseCallback {

    void receiveResponse(HbaseObjectWritable value);

    void receiveError(RemoteError remoteError);

}
