package org.wizbang.hbase.nbhc.netty.pipeline;

import com.google.common.primitives.Ints;
import org.wizbang.hbase.nbhc.dispatch.Request;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

public final class HbaseRequestEncoder extends OneToOneEncoder {

    private static final Logger log = LogManager.getLogger(HbaseRequestEncoder.class);

    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object message) throws Exception {
        if (!(message instanceof Request)) {
            return message;
        }

        Request request = (Request) message;

        Invocation invocation = request.getInvocation();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(baos);
        invocation.write(dataOutput);

        byte[] raw = baos.toByteArray();
        int dataLength = Ints.BYTES + raw.length;

        if (log.isDebugEnabled()) {
            log.debug("Sending request with id " + request.getRequestId());
        }

        ChannelBuffer buffer = ChannelBuffers.buffer(Ints.BYTES + dataLength);
        buffer.writeInt(dataLength);
        buffer.writeInt(request.getRequestId());
        buffer.writeBytes(raw);

        return buffer;
    }
}
