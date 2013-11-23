package org.wizbang.hbase.nbhc.response;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public final class Response {

    public enum Type {
        REMOTE_ERROR, LOCAL_ERROR, VALUE
    }

    private final int requestId;
    private final Type type;
    private final RemoteError error;
    private final Throwable localError;
    private final HbaseObjectWritable value;

    public static Response newRemoteError(int requestId, RemoteError error) {
        return new Response(requestId, Type.REMOTE_ERROR, error, null, null);
    }

    public static Response newLocalError(int requestId, Throwable localError) {
        return new Response(requestId, Type.LOCAL_ERROR, null, localError, null);
    }

    public static Response newResponse(int requestId, HbaseObjectWritable value) {
        return new Response(requestId, Type.VALUE, null, null, value);
    }

    private Response(int requestId, Type type, RemoteError error, Throwable localError, HbaseObjectWritable value) {
        this.type = type;
        this.requestId = requestId;
        this.error = error;
        this.localError = localError;
        this.value = value;
    }

    public int getRequestId() {
        return requestId;
    }

    public Type getType() {
        return type;
    }

    public RemoteError getRemoteError() {
        Preconditions.checkState(type == Type.REMOTE_ERROR);
        return error;
    }

    public Throwable getLocalError() {
        Preconditions.checkState(type == Type.LOCAL_ERROR);
        return localError;
    }

    public HbaseObjectWritable getValue() {
        Preconditions.checkState(type == Type.VALUE);
        return value;
    }
}
