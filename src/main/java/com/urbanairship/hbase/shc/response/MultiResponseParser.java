package com.urbanairship.hbase.shc.response;

import com.urbanairship.hbase.shc.dispatch.ResultBroker;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;

import java.util.List;
import java.util.Map;

public final class MultiResponseParser implements ResponseCallback {

    private final ResultBroker<?> broker;
    private final String operationName;

    public MultiResponseParser(ResultBroker<?> broker, String operationName) {
        this.broker = broker;
        this.operationName = operationName;
    }

    @Override
    public void receiveResponse(HbaseObjectWritable value) {
        Object result = value.get();
        if (!(result instanceof MultiResponse)) {
            broker.communicateError(new RuntimeException(String.format("Expected response value of %s but received %s for 'multi %s' operation",
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
            broker.communicateError(new RuntimeException(String.format("Errors detected in response of 'multi %s' operation", operationName)));
        }

        broker.communicateResult(null);
    }

    @Override
    public void receiveError(ResponseError responseError) {
        // TODO: better error handling
        broker.communicateError(new RemoteException(responseError.getErrorClass(),
                responseError.getErrorMessage().isPresent()
                    ? responseError.getErrorMessage().get()
                    : ""));
    }
}
