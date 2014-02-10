package org.wizbang.hbase.nbhc.netty.pipeline;

import org.wizbang.hbase.nbhc.netty.DisconnectCallback;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

// TODO: probably need to hold onto the request ids that are on a particular channel and then if the we get the
// TODO: channel disconnected, we should get the controller for those request ids and issue a retriable error to
// TODO: them.
public final class HbaseRegionServerConnectionHandler extends SimpleChannelHandler {

    private static final Logger log = LogManager.getLogger(HbaseRegionServerConnectionHandler.class);

    private final DisconnectCallback disconnectCallback;

    public HbaseRegionServerConnectionHandler(DisconnectCallback disconnectCallback) {
        this.disconnectCallback = disconnectCallback;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        Channel channel = e.getChannel();

        if (log.isDebugEnabled()) {
            log.debug(String.format("Connection established from %s to %s", channel.getLocalAddress().toString(),
                    channel.getRemoteAddress().toString()));
        }

        super.channelConnected(ctx, e);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        Channel channel = e.getChannel();

        if (log.isDebugEnabled()) {
            log.debug(String.format("Connection from %s to %s disconnected", channel.getLocalAddress().toString(),
                    channel.getRemoteAddress().toString()));
        }

        disconnectCallback.disconnected(channel);

        super.channelDisconnected(ctx, e);
    }
}
