package com.urbanairship.hbase.shc.dispatch;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public interface ResponseCallback {

    void receiveResponse(HbaseObjectWritable value);

    void receiveError(Throwable e);

}
