package org.wizbang.hbase.nbhc.response;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public interface RequestResponseController {

    void receiveResponse(int requestId, HbaseObjectWritable value);

    void receiveRemoteError(int requestId, RemoteError remoteError);

    void receiveLocalError(int requestId, Throwable error);

    void receiveFatalError(int requestId, Throwable error);

    void cancel();

}
