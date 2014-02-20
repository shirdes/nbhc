package org.wizbang.hbase.nbhc.netty;

import com.codahale.metrics.Timer;
import com.google.common.net.HostAndPort;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.wizbang.hbase.nbhc.HbaseClientMetrics;
import org.wizbang.hbase.nbhc.dispatch.RegionServerDispatcher;
import org.wizbang.hbase.nbhc.dispatch.Request;

public final class NettyRegionServerDispatcher implements RegionServerDispatcher {

    private static final Timer REQUESTS_DISPATCHED_TIMER = HbaseClientMetrics.timer("NettyRegionServerDispatcher:RequestsDispatched");

    private final HostChannelProvider channelProvider;

    public NettyRegionServerDispatcher(HostChannelProvider channelProvider) {
        this.channelProvider = channelProvider;
    }

    @Override
    public void request(HostAndPort host, Request request) {
        Timer.Context hostTimer = HbaseClientMetrics.timer("NettyRegionServerDispatcher:Host:" + host.toString()).time();
        Timer.Context time = REQUESTS_DISPATCHED_TIMER.time();
        try {
            Channel channel = channelProvider.getChannel(host);
            Channels.write(channel, request);
        }
        finally {
            time.stop();
            hostTimer.stop();
        }
    }
}
