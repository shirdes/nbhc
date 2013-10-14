package com.urbanairship.hbase.shc.dispatch.netty.pipeline;

import com.urbanairship.hbase.shc.dispatch.RequestManager;
import com.urbanairship.hbase.shc.dispatch.netty.DisconnectCallback;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

public class HbaseClientPipelineFactory implements ChannelPipelineFactory {

    private final RequestManager requestManager;
    private final DisconnectCallback disconnectCallback;

    public HbaseClientPipelineFactory(RequestManager requestManager, DisconnectCallback disconnectCallback) {
        this.requestManager = requestManager;
        this.disconnectCallback = disconnectCallback;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        final ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("connection-handler", new HbaseRegionServerConnectionHandler(disconnectCallback));
        pipeline.addLast("frame-decoder", new LengthFieldBasedFrameDecoder(
                HbaseResponseDecoder.MAX_RESPONSE_LENGTH,
                HbaseResponseDecoder.FRAME_LENGTH_FIELD_POSITION,
                HbaseResponseDecoder.LENGTH_FIELD_LENGTH,
                ((-1) * (HbaseResponseDecoder.FRAME_LENGTH_FIELD_POSITION + HbaseResponseDecoder.LENGTH_FIELD_LENGTH)),
                0)
        );
        pipeline.addLast("response-decoder", new HbaseResponseDecoder());
        pipeline.addLast("connection-hello-message-encoder", new HbaseRegionConnectionHelloMessageEncoder());
        pipeline.addLast("request-encoder", new HbaseRequestEncoder());
        pipeline.addLast("response-handler", new HbaseResponseHandler(requestManager));
        return pipeline;
    }
}
