package org.wizbang.hbase.nbhc.request.multi;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.response.RequestResponseController;

public final class MultiActionRequestResponseController<P extends Row> implements RequestResponseController {

    private final MultiActionCoordinator<P> coordinator;

    public MultiActionRequestResponseController(MultiActionCoordinator<P> coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void receiveResponse(HbaseObjectWritable received) {
        Object responseObject = received.get();
        if (!(responseObject instanceof MultiResponse)) {
            coordinator.processUnrecoverableError(new RuntimeException(String.format("Expected response of type %s but received %s",
                    MultiResponse.class.getName(), responseObject.getClass().getName())));
            return;
        }

        MultiResponse response = (MultiResponse) responseObject;

        ImmutableSet.Builder<Integer> needRetry = ImmutableSet.builder();
        ImmutableMap.Builder<Integer, Result> results = ImmutableMap.builder();
        Optional<Throwable> failure = Optional.absent();

        // TODO: is there a way that we can determine the host that the result object si from so that we can provide
        // TODO: the host in the error message if there is one?  The default client does this...
        Iterable<Pair<Integer, Object>> pairs = Iterables.concat(response.getResults().values());
        for (Pair<Integer, Object> pair : pairs) {
            Object result = pair.getSecond();
            if (result == null) {
                needRetry.add(pair.getFirst());
            }
            else if (result instanceof Throwable) {
                Throwable error = (Throwable) result;
                if (!(error instanceof DoNotRetryIOException) &&
                        (error instanceof NotServingRegionException || error instanceof RegionServerStoppedException)) {
                    needRetry.add(pair.getFirst());
                }
                else {
                    failure = Optional.of(error);
                    break;
                }
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
            coordinator.processUnrecoverableError(failure.get());
        }
        else {
            coordinator.processResponseResult(results.build(), needRetry.build());
        }
    }

    @Override
    public void receiveRemoteError(RemoteError remoteError) {
        coordinator.processUnrecoverableError(new RemoteException(remoteError.getErrorClass(),
                (remoteError.getErrorMessage().isPresent() ? remoteError.getErrorMessage().get() : "")));
    }

    @Override
    public void receiveLocalError(Throwable error) {
        coordinator.processUnrecoverableError(error);
    }
}
