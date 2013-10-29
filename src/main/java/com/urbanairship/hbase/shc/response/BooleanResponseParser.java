package com.urbanairship.hbase.shc.response;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public final class BooleanResponseParser implements Function<HbaseObjectWritable, Boolean> {

    private final String operationName;

    public BooleanResponseParser(String operationName) {
        this.operationName = operationName;
    }

    @Override
    public Boolean apply(HbaseObjectWritable value) {
        Object object = value.get();
        if (!(object instanceof Boolean)) {
            throw new RuntimeException(String.format("Expected result of %s but received %s for '%s' operation",
                    Boolean.class.getName(), object.getClass().getName(), operationName));
        }

        return (Boolean) object;
    }
}
