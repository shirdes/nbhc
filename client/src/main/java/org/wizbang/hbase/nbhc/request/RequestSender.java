package org.wizbang.hbase.nbhc.request;

import com.google.common.net.HostAndPort;
import org.wizbang.hbase.nbhc.Operation;
import org.wizbang.hbase.nbhc.dispatch.RegionServerDispatcher;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.response.ResponseCallback;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;

public class RequestSender {

    private final RegionServerDispatcher dispatcher;

    public RequestSender(RegionServerDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void sendRequest(HRegionLocation location,
                            Invocation invocation,
                            ResultBroker<?> resultBroker,
                            final RequestController controller,
                            final int attempt) {

        Operation operation = new Operation(getHost(location), invocation);

        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void receiveResponse(HbaseObjectWritable value) {
                controller.handleResponse(value);
            }

            @Override
            public void receiveRemoteError(RemoteError remoteError) {
                controller.handleRemoteError(remoteError, attempt);
            }

            @Override
            public void receiveLocalError(Throwable error) {
                controller.handleLocalError(error, attempt);
            }
        };

        int requestId = dispatcher.request(operation, callback);
        resultBroker.setCurrentActiveRequestId(requestId);
    }

    private HostAndPort getHost(HRegionLocation location) {
        return HostAndPort.fromParts(location.getHostname(), location.getPort());
    }
}
