package org.wizbang.hbase.nbhc.netty;

import com.codahale.metrics.Timer;
import org.wizbang.hbase.nbhc.HbaseClientMetrics;
import org.wizbang.hbase.nbhc.Operation;
import org.wizbang.hbase.nbhc.dispatch.RegionServerDispatcher;
import org.wizbang.hbase.nbhc.dispatch.Request;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.response.ResponseCallback;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;

public final class NettyRegionServerDispatcher implements RegionServerDispatcher {

    private static final Timer REQUESTS_DISPATCHED_TIMER = HbaseClientMetrics.timer("NettyRegionServerDispatcher:RequestsDispatched");

    private final RequestManager requestManager;
    private final HostChannelProvider channelProvider;

    public NettyRegionServerDispatcher(RequestManager requestManager,
                                       HostChannelProvider channelProvider) {
        this.requestManager = requestManager;
        this.channelProvider = channelProvider;
    }

    @Override
    public int request(Operation operation, ResponseCallback callback) {
        Timer.Context hostTimer =
                HbaseClientMetrics.timer("NettyRegionServerDispatcher:Host:" + operation.getTargetHost().toString()).time();
        Timer.Context time = REQUESTS_DISPATCHED_TIMER.time();
        try {
            Channel channel = channelProvider.getChannel(operation.getTargetHost());

            int requestId = requestManager.registerResponseCallback(callback);
            // TODO: not sure if the error handling will ensure that the callback will be removed if there is some low
            // TODO: level socket error or something like that?
            Channels.write(channel, new Request(requestId, operation));

            return requestId;
        }
        finally {
            time.stop();
            hostTimer.stop();
        }
    }
}
