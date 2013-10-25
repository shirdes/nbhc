package com.urbanairship.hbase.shc.request;

import com.google.common.base.Function;
import com.urbanairship.hbase.shc.dispatch.ResultBroker;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public final class SimpleParseResponseProcessor<R> implements ResponseProcessor<R> {

    private final Function<HbaseObjectWritable, R> parser;

    public SimpleParseResponseProcessor(Function<HbaseObjectWritable, R> parser) {
        this.parser = parser;
    }

    @Override
    public void process(HRegionLocation location, HbaseObjectWritable received, ResultBroker<R> resultBroker) {
        R response;
        try {
            response = parser.apply(received);
        }
        catch (Exception e) {
            resultBroker.communicateError(e);
            return;
        }

        resultBroker.communicateResult(response);
    }
}
