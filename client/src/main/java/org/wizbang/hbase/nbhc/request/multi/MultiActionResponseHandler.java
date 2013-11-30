package org.wizbang.hbase.nbhc.request.multi;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.wizbang.hbase.nbhc.request.ResponseHandler;
import org.wizbang.hbase.nbhc.response.RemoteError;

public final class MultiActionResponseHandler<P extends Row> implements ResponseHandler {

    private final MultiActionController<P> controller;

    public MultiActionResponseHandler(MultiActionController<P> controller) {
        this.controller = controller;
    }

    @Override
    public void handleResponse(HbaseObjectWritable received) {
        Object responseObject = received.get();
        if (!(responseObject instanceof MultiResponse)) {
            controller.processUnrecoverableError(new RuntimeException(String.format("Expected response of type %s but received %s",
                    MultiResponse.class.getName(), responseObject.getClass().getName())));
            return;
        }

        MultiResponse response = (MultiResponse) responseObject;

        ImmutableSet.Builder<Integer> needRetry = ImmutableSet.builder();
        ImmutableMap.Builder<Integer, Result> results = ImmutableMap.builder();
        Optional<Throwable> failure = Optional.absent();

        Iterable<Pair<Integer, Object>> pairs = Iterables.concat(response.getResults().values());
        for (Pair<Integer, Object> pair : pairs) {
            Object result = pair.getSecond();
            if (result == null || result instanceof Throwable) {
                // TODO: need to do a check to see if the error can be retried.  Shouldn't retry in the case of a DoNotRetryIOException
                // TODO: verify we found a param object at the index
                needRetry.add(pair.getFirst());
            }
            else if (result instanceof Result) {
                results.put(pair.getFirst(), (Result) result);
            }
            else {
                failure = Optional.<Throwable>of(new RuntimeException("Received unknown response object of type " + result.getClass()));
                break;
            }
        }

        if (failure.isPresent()) {
            controller.processUnrecoverableError(failure.get());
        }
        else {
            controller.processResponseResult(results.build(), needRetry.build());
        }
    }

    @Override
    public void handleRemoteError(RemoteError error, int attempt) {
        // TODO: check if the error is retryable and get smarter about what we return perhaps
//        resultBroker.communicateError(new RemoteException(error.getErrorClass(),
//                error.getErrorMessage().isPresent() ? error.getErrorMessage().get() : ""));
    }

    @Override
    public void handleLocalError(Throwable error, int attempt) {
        controller.processUnrecoverableError(error);
    }
}
