package org.wizbang.hbase.nbhc.netty.pipeline;

import com.google.common.primitives.Ints;
import org.wizbang.hbase.nbhc.dispatch.ConnectionHelloMessage;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

public final class HbaseRegionConnectionHelloMessageEncoder extends OneToOneEncoder {

    private static final Logger log = LogManager.getLogger(HbaseRegionConnectionHelloMessageEncoder.class);

    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object message) throws Exception {
        if (!(message instanceof ConnectionHelloMessage)) {
            return message;
        }

        ConnectionHelloMessage hello = (ConnectionHelloMessage) message;

        int bufferLength = hello.getMagic().length +
                           1 +
                           Ints.BYTES +
                           1 + hello.getProtocol().length;

        ChannelBuffer buffer = ChannelBuffers.buffer(bufferLength);
        buffer.writeBytes(hello.getMagic());
        buffer.writeByte(hello.getVersion());

        buffer.writeInt(hello.getProtocol().length);

        buffer.writeBytes(hello.getProtocol());

        if (log.isDebugEnabled()) {
            log.debug(String.format("Sending initial header to region for connection from %s to %s",
                    channel.getLocalAddress().toString(), channel.getRemoteAddress().toString()));
        }

        return buffer;
    }
}
