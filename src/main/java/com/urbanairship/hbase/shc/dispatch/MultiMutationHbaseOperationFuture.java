package com.urbanairship.hbase.shc.dispatch;

import com.google.common.util.concurrent.AbstractFuture;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;
import java.util.Map;

public final class MultiMutationHbaseOperationFuture extends AbstractFuture<Void> implements ResponseCallback {

    private final String operationName;

    public MultiMutationHbaseOperationFuture(String operationName) {
        this.operationName = operationName;
    }

    @Override
    public void receiveResponse(HbaseObjectWritable value) {
        Object result = value.get();
        if (!(result instanceof MultiResponse)) {
            setException(new RuntimeException(String.format("Expected response value of %s but received %s for 'multi %s' operation",
                    MultiResponse.class.getName(), result.getClass().getName(), operationName)));
            return;
        }

        boolean failed = false;
        MultiResponse response = (MultiResponse) result;
        for (Map.Entry<byte[], List<Pair<Integer, Object>>> entry : response.getResults().entrySet()) {
            for (Pair<Integer, Object> pair : entry.getValue()) {
                // TODO: existing client will retry if the second is null or it's a Throwable but not a DNRIE
                if (pair == null || (pair.getSecond() instanceof Throwable)) {
                    failed = true;
                    break;
                }
            }
        }

        if (failed) {
            // TODO: would be good to have the exceptions that occurred?
            setException(new RuntimeException(String.format("Errors detected in response of 'multi %s' operation", operationName)));
        }

        set(null);
    }

    @Override
    public void receiveError(Throwable e) {
        setException(e);
    }
}
