package org.wizbang.hbase.nbhc.request;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.hbase.HRegionLocation;
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

    public int sendRequest(RequestDetailProvider requestDetailProvider,
                           RequestResponseController controller) {
        return send(requestDetailProvider.getLocation(), requestDetailProvider, controller);
    }

    public int retryRequest(RequestDetailProvider requestDetailProvider,
                            RequestResponseController controller) {
        return send(requestDetailProvider.getRetryLocation(), requestDetailProvider, controller);
    }

    private int send(HRegionLocation location,
                     RequestDetailProvider requestDetailProvider,
                     RequestResponseController controller) {

        int requestId = requestManager.registerController(controller);

        Invocation invocation = requestDetailProvider.getInvocation(location);
        Request request = new Request(requestId, invocation);
        HostAndPort host = HostAndPort.fromParts(location.getHostname(), location.getPort());
        dispatcher.request(host, request);

        return requestId;
    }
}
