package com.urbanairship.hbase.shc.dispatch.netty;

import com.urbanairship.hbase.shc.Operation;
import com.urbanairship.hbase.shc.dispatch.RegionServerDispatcher;
import com.urbanairship.hbase.shc.dispatch.Request;
import com.urbanairship.hbase.shc.dispatch.RequestManager;
import com.urbanairship.hbase.shc.response.ResponseCallback;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;

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
        Channel channel = channelProvider.getChannel(operation.getTargetHost());

        int requestId = requestManager.registerResponseCallback(callback);
        // TODO: not sure if the error handling will ensure that the callback will be removed if there is some low
        // TODO: level socket error or something like that?
        Channels.write(channel, new Request(requestId, operation));

        return requestId;
    }
}
