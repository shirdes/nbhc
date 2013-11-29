package org.wizbang.hbase.nbhc.request;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.wizbang.hbase.nbhc.Operation;
import org.wizbang.hbase.nbhc.dispatch.RegionServerDispatcher;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.response.ResponseCallback;

public class RequestSender {

    private final RegionServerDispatcher dispatcher;

    public RequestSender(RegionServerDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void sendRequest(HRegionLocation location,
                            Invocation invocation,
                            final ResponseHandler responseHandler,
                            final int attempt) {
        dispatch(location, invocation, responseHandler, attempt);
    }

    public void sendRequest(HRegionLocation location,
                            Invocation invocation,
                            ResultBroker<?> resultBroker,
                            final ResponseHandler responseHandler,
                            final int attempt) {

        int requestId = dispatch(location, invocation, responseHandler, attempt);
        resultBroker.setCurrentActiveRequestId(requestId);
    }

    private int dispatch(HRegionLocation location,
                         Invocation invocation,
                         final ResponseHandler responseHandler,
                         final int attempt) {
        Operation operation = new Operation(getHost(location), invocation);

        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void receiveResponse(HbaseObjectWritable value) {
                responseHandler.handleResponse(value);
            }

            @Override
            public void receiveRemoteError(RemoteError remoteError) {
                responseHandler.handleRemoteError(remoteError, attempt);
            }

            @Override
            public void receiveLocalError(Throwable error) {
                responseHandler.handleLocalError(error, attempt);
            }
        };

        return dispatcher.request(operation, callback);
    }

    private HostAndPort getHost(HRegionLocation location) {
        return HostAndPort.fromParts(location.getHostname(), location.getPort());
    }
}
