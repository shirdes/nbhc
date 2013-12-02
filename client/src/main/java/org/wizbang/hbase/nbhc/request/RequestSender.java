package org.wizbang.hbase.nbhc.request;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.wizbang.hbase.nbhc.dispatch.RegionServerDispatcher;
import org.wizbang.hbase.nbhc.dispatch.Request;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.response.ResponseCallback;

public class RequestSender {

    private final RequestManager requestManager;
    private final RegionServerDispatcher dispatcher;

    public RequestSender(RequestManager requestManager, RegionServerDispatcher dispatcher) {
        this.requestManager = requestManager;
        this.dispatcher = dispatcher;
    }

    public int sendRequest(HRegionLocation location,
                                       Invocation invocation,
                                       final ResponseHandler responseHandler,
                                       final int attempt) {

        ResponseCallback responseCallback = new ResponseCallback() {
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

        final int requestId = requestManager.registerResponseCallback(responseCallback);

        Request request = new Request(requestId, invocation);
        dispatcher.request(getHost(location), request);

        return requestId;
    }

    public void sendRequestForBroker(HRegionLocation location,
                                     Invocation invocation,
                                     ResultBroker<?> resultBroker,
                                     final ResponseHandler responseHandler,
                                     final int attempt) {

        int requestId = sendRequest(location, invocation, responseHandler, attempt);
        resultBroker.setCurrentActiveRequestId(requestId);
    }

    private HostAndPort getHost(HRegionLocation location) {
        return HostAndPort.fromParts(location.getHostname(), location.getPort());
    }
}
