package org.wizbang.hbase.nbhc.netty.pipeline;

import com.codahale.metrics.Meter;
import com.google.common.base.Optional;
import org.wizbang.hbase.nbhc.HbaseClientMetrics;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.netty.HostChannelProvider;
import org.wizbang.hbase.nbhc.response.RequestResponseController;
import org.wizbang.hbase.nbhc.response.Response;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public class HbaseResponseHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = LogManager.getLogger(HbaseResponseHandler.class);

    private static final Meter RESPONSES_RECEIVED_METER = HbaseClientMetrics.meter("HbaseResponseHandler:ResponsesReceived");

    private final RequestManager requestManager;
    private final HostChannelProvider channelProvider;

    public HbaseResponseHandler(RequestManager requestManager, HostChannelProvider channelProvider) {
        this.requestManager = requestManager;
        this.channelProvider = channelProvider;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object message = e.getMessage();
        if (!(message instanceof Response)) {
            super.messageReceived(ctx, e);
            return;
        }

        RESPONSES_RECEIVED_METER.mark();

        Response response = (Response) message;
        Optional<RequestResponseController> lookup = requestManager.retrieveCallback(response.getRequestId());
        if (!lookup.isPresent()) {
            return;
        }

        RequestResponseController callback = lookup.get();
        switch (response.getType()) {
            case LOCAL_ERROR:
                callback.receiveLocalError(response.getLocalError());
                break;
            case REMOTE_ERROR:
                callback.receiveRemoteError(response.getRemoteError());
                break;
            case VALUE:
                callback.receiveResponse(response.getValue());
                break;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        log.error("Exception caught in response handler.  Removing channel.", e.getCause());
        channelProvider.removeChannel(ctx.getChannel());
    }
}
