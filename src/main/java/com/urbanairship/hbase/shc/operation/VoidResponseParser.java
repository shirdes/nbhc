package com.urbanairship.hbase.shc.operation;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public final class VoidResponseParser implements Function<HbaseObjectWritable, Void> {

    private final String operationName;

    public VoidResponseParser(String operationName) {
        this.operationName = operationName;
    }

    @Override
    public Void apply(HbaseObjectWritable value) {
        Class<?> resultType = value.getDeclaredClass();
        if (!Void.TYPE.equals(resultType)) {
            throw new RuntimeException(String.format("Expected response value of %s but received %s for '%s' operation",
                    Void.class.getName(), resultType.getName(), operationName));
        }

        return null;
    }
}
