package com.urbanairship.hbase.shc.dispatch;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AbstractFuture;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public final class SimpleResponseParsingHbaseOperationFuture<R> extends AbstractFuture<R> implements ResponseCallback {

    private final Function<HbaseObjectWritable, R> responseValueParser;

    public SimpleResponseParsingHbaseOperationFuture(Function<HbaseObjectWritable, R> responseValueParser) {
        this.responseValueParser = responseValueParser;
    }

    @Override
    public void receiveResponse(HbaseObjectWritable value) {
        try {
            R response = responseValueParser.apply(value);
            set(response);
        }
        catch (Throwable e) {
            setException(e);
        }
    }

    @Override
    public void receiveError(Throwable e) {
        setException(e);
    }
}
