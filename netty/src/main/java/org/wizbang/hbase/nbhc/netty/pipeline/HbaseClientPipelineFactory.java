package org.wizbang.hbase.nbhc.netty.pipeline;

import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.netty.DisconnectCallback;
import org.wizbang.hbase.nbhc.netty.HostChannelProvider;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

public final class HbaseClientPipelineFactory implements ChannelPipelineFactory {

    private final RequestManager requestManager;
    private final DisconnectCallback disconnectCallback;
    private final HostChannelProvider channelProvider;

    public HbaseClientPipelineFactory(RequestManager requestManager,
                                      DisconnectCallback disconnectCallback,
                                      HostChannelProvider channelProvider) {
        this.requestManager = requestManager;
        this.disconnectCallback = disconnectCallback;
        this.channelProvider = channelProvider;
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
        pipeline.addLast("response-handler", new HbaseResponseHandler(requestManager, channelProvider));
        return pipeline;
    }
}
