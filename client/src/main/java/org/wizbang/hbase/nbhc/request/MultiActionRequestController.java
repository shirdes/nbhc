package org.wizbang.hbase.nbhc.request;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class MultiActionRequestController<P> implements RequestController {

    private final Function<Integer, P> indexParamLookup;
    private final ResultBroker<ImmutableMap<Integer, Result>> resultBroker;

    private final ConcurrentMap<Integer, Result> collectedResults = new ConcurrentHashMap<Integer, Result>();

    public MultiActionRequestController(Function<Integer, P> indexParamLookup,
                                        ResultBroker<ImmutableMap<Integer, Result>> resultBroker) {
        this.indexParamLookup = indexParamLookup;
        this.resultBroker = resultBroker;
    }

    @Override
    public void handleResponse(HbaseObjectWritable received) {
        Object responseObject = received.get();
        if (!(responseObject instanceof MultiResponse)) {
            // TODO: should probably bomb out
            return;
        }

        MultiResponse response = (MultiResponse) responseObject;

        Map<Integer, P> needRetry = Maps.newHashMap();
        Iterable<Pair<Integer, Object>> pairs = Iterables.concat(response.getResults().values());
        for (Pair<Integer, Object> pair : pairs) {
            Object result = pair.getSecond();
            if (result == null || result instanceof Throwable) {
                // TODO: need to do a check to see if the error can be retried.  Shouldn't retry in the case of a DoNotRetryIOException
                // TODO: verify we found a param object at the index
                needRetry.put(pair.getFirst(), indexParamLookup.apply(pair.getFirst()));
            }
            else if (result instanceof Result) {
                collectedResults.put(pair.getFirst(), (Result) result);
            }
            else {
                resultBroker.communicateError(new RuntimeException("Received unknown response object of type " +
                        result.getClass()));
                // TODO: this return is real deep.
                return;
            }
        }

        // TODO: if needRetry has stuff in it, then need to issue the request for those rows.

        // TODO: if we find that we need to do retries, we need a way to know when we've collected enough results to
        // TODO: be completely done.
        if (needRetry.isEmpty()) {
            resultBroker.communicateResult(ImmutableMap.copyOf(collectedResults));
        }
        else {
            // TODO: do retries but for now, we'll just return what we got!
            resultBroker.communicateResult(ImmutableMap.copyOf(collectedResults));
        }
    }

    @Override
    public void handleRemoteError(RemoteError error, int attempt) {
        // TODO: check if the error is retryable and get smarter about what we return perhaps
        resultBroker.communicateError(new RemoteException(error.getErrorClass(),
                error.getErrorMessage().isPresent() ? error.getErrorMessage().get() : ""));
    }

    @Override
    public void handleLocalError(Throwable error, int attempt) {
        // TODO: should we retry?
        resultBroker.communicateError(error);
    }
}
