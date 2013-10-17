package com.urbanairship.hbase.shc.response;

import com.google.common.base.Optional;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public final class Response {

    private final int requestId;
    private final Optional<ResponseError> error;
    private final Optional<HbaseObjectWritable> value;

    public static Response newError(int requestId, ResponseError error) {
        return new Response(requestId, Optional.of(error), Optional.<HbaseObjectWritable>absent());
    }

    public static Response newResponse(int requestId, HbaseObjectWritable value) {
        return new Response(requestId, Optional.<ResponseError>absent(), Optional.of(value));
    }

    private Response(int requestId, Optional<ResponseError> error, Optional<HbaseObjectWritable> value) {
        this.requestId = requestId;
        this.error = error;
        this.value = value;
    }

    public int getRequestId() {
        return requestId;
    }

    public Optional<ResponseError> getError() {
        return error;
    }

    public Optional<HbaseObjectWritable> getValue() {
        return value;
    }
}
