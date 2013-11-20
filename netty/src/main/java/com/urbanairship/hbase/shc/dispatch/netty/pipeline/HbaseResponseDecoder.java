package com.urbanairship.hbase.shc.dispatch.netty.pipeline;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.urbanairship.hbase.shc.response.RemoteError;
import com.urbanairship.hbase.shc.response.Response;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import java.io.IOException;

public final class HbaseResponseDecoder extends OneToOneDecoder {

    private static final Logger log = LogManager.getLogger(HbaseResponseDecoder.class);

    // TODO: can we come up with a better limit than this??
    public static final int MAX_RESPONSE_LENGTH = Integer.MAX_VALUE;

    public static final int FRAME_LENGTH_FIELD_POSITION = Ints.BYTES + 1;

    public static final int LENGTH_FIELD_LENGTH = Ints.BYTES;

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, Object message) throws Exception {
        if (!(message instanceof ChannelBuffer)) {
            return message;
        }

        ChannelBuffer buffer = (ChannelBuffer) message;
        return parseResponse(buffer);
    }

    private Response parseResponse(ChannelBuffer buffer) {
        int requestId = buffer.readInt();
        byte flags = buffer.readByte();

        boolean isErrorResponse = hasErrorFlag(flags);

        // Skip the length
        buffer.readInt();

        // Skip the state
        buffer.readInt();

        if (isErrorResponse) {
            return Response.newRemoteError(requestId, readError(buffer));
        }

        HbaseObjectWritable value;
        try {
            value = readResponseValue(buffer);
        }
        catch (Exception e) {
            return Response.newLocalError(requestId,
                    new RuntimeException("Error parsing HbaseObjectWritable from binary response" + requestId, e));
        }

        return Response.newResponse(requestId, value);
    }

    private HbaseObjectWritable readResponseValue(ChannelBuffer buffer) throws IOException {
        ChannelBufferInputStream in = new ChannelBufferInputStream(buffer);

        HbaseObjectWritable how = new HbaseObjectWritable();
        how.readFields(in);

        return how;
    }

    private RemoteError readError(ChannelBuffer buffer) {
        String exceptionClass = readFramedString(buffer);
        String exceptionMessage = readFramedString(buffer);

        return new RemoteError(exceptionClass, StringUtils.isNotBlank(exceptionMessage)
            ? Optional.of(exceptionMessage) : Optional.<String>absent());
    }

    private boolean hasErrorFlag(byte flags) {
        return (flags & 0x1) != 0;
    }

    private String readFramedString(ChannelBuffer buffer) {
        int length = buffer.readInt();
        Preconditions.checkArgument(length > 0);

        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);

        return new String(bytes, Charsets.UTF_8);
    }
}
