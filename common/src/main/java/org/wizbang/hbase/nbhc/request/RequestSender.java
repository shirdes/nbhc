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

    public int sendRequest(HRegionLocation location,
                           Invocation invocation,
                           RequestResponseController controller) {
        int requestId = requestManager.registerController(controller);

        // TODO: the dispatch should maybe be done on another thread so that it's async and then if the dispatch fails, we
        // TODO: have the opportunity to callback to the controller and let it know that so that the controller can
        // TODO: then retry if necessary? I suppose that we can issue the failure to the controller on this thread
        // TODO: technically - which as things currently stand, is going to be the caller's thread - but seems a little
        // TODO: sketch and kind of makes the request less "async"...
        Request request = new Request(requestId, invocation);
        HostAndPort host = HostAndPort.fromParts(location.getHostname(), location.getPort());
        dispatcher.request(host, request);

        return requestId;
    }
}
