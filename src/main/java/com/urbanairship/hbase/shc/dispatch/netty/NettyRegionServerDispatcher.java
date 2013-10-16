package com.urbanairship.hbase.shc.dispatch.netty;

import com.urbanairship.hbase.shc.dispatch.RegionServerDispatcher;
import com.urbanairship.hbase.shc.dispatch.Request;
import com.urbanairship.hbase.shc.dispatch.RequestManager;
import com.urbanairship.hbase.shc.dispatch.ResponseCallback;
import com.urbanairship.hbase.shc.operation.Operation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;

public final class NettyRegionServerDispatcher implements RegionServerDispatcher {

    private static final Logger log = LogManager.getLogger(NettyRegionServerDispatcher.class);

    private final RequestManager requestManager;
    private final HostChannelProvider channelProvider;

    public NettyRegionServerDispatcher(RequestManager requestManager,
                                       HostChannelProvider channelProvider) {
        this.requestManager = requestManager;
        this.channelProvider = channelProvider;
    }

    @Override
    public int request(Operation operation, ResponseCallback callback) {
        int requestId = requestManager.registerResponseCallback(callback);

        Channel channel = channelProvider.getChannel(operation.getTargetHost());
        channel.write(new Request(requestId, operation));

        return requestId;
    }
}
