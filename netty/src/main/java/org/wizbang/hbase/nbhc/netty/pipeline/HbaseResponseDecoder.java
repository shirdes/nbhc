package org.wizbang.hbase.nbhc.netty.pipeline;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.response.Response;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import java.io.IOException;

public final class HbaseResponseDecoder extends OneToOneDecoder {

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
        int requestId = buffer.readInt();

        Response response;
        try {
            response = parseResponseDetail(buffer, requestId);
        }
        catch (Exception e) {
            response = Response.newLocalError(requestId,
                    new RuntimeException("Error parsing response detail for request id " + requestId));
        }

        return response;
    }

    private Response parseResponseDetail(ChannelBuffer buffer, int requestId) throws IOException {
        byte flags = buffer.readByte();

        boolean isErrorResponse = hasErrorFlag(flags);

        // Skip the length
        buffer.readInt();

        // Skip the state
        buffer.readInt();

        if (isErrorResponse) {
            return Response.newRemoteError(requestId, readError(buffer));
        }

        HbaseObjectWritable value = readResponseValue(buffer);

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
