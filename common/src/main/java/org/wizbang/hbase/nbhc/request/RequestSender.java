package org.wizbang.hbase.nbhc.request;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.wizbang.hbase.nbhc.dispatch.RegionServerDispatcher;
import org.wizbang.hbase.nbhc.dispatch.Request;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.response.RequestResponseController;

public class RequestSender {

    private final RequestManager requestManager;
    private final RegionServerDispatcher dispatcher;

    public RequestSender(RequestManager requestManager, RegionServerDispatcher dispatcher) {
        this.requestManager = requestManager;
        this.dispatcher = dispatcher;
    }

    public int sendRequest(HostAndPort host,
                           Invocation invocation,
                           RequestResponseController controller) {
        int requestId = requestManager.registerController(controller);

        Request request = new Request(requestId, invocation);
        dispatcher.request(host, request);

        return requestId;
    }
}
