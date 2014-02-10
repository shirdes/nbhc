package org.wizbang.hbase.nbhc.response;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public final class Response {

    public enum Type {
        REMOTE_ERROR, FATAL_ERROR, VALUE
    }

    private final int requestId;
    private final Type type;
    private final RemoteError error;
    private final Throwable fatalError;
    private final HbaseObjectWritable value;

    public static Response newRemoteError(int requestId, RemoteError error) {
        return new Response(requestId, Type.REMOTE_ERROR, error, null, null);
    }

    public static Response newFatalError(int requestId, Throwable localError) {
        return new Response(requestId, Type.FATAL_ERROR, null, localError, null);
    }

    public static Response newResponse(int requestId, HbaseObjectWritable value) {
        return new Response(requestId, Type.VALUE, null, null, value);
    }

    private Response(int requestId, Type type, RemoteError error, Throwable fatalError, HbaseObjectWritable value) {
        this.type = type;
        this.requestId = requestId;
        this.error = error;
        this.fatalError = fatalError;
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

    public Throwable getFatalError() {
        Preconditions.checkState(type == Type.FATAL_ERROR);
        return fatalError;
    }

    public HbaseObjectWritable getValue() {
        Preconditions.checkState(type == Type.VALUE);
        return value;
    }
}
