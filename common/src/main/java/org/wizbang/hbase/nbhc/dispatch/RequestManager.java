package org.wizbang.hbase.nbhc.dispatch;

import com.codahale.metrics.Gauge;
import com.google.common.base.Optional;
import org.wizbang.hbase.nbhc.HbaseClientMetrics;
import org.wizbang.hbase.nbhc.response.ResponseCallback;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestManager {

    private final AtomicInteger ids = new AtomicInteger(0);

    private final ConcurrentMap<Integer, ResponseCallback> responseCallbacks = new ConcurrentHashMap<Integer, ResponseCallback>();

    public RequestManager() {
        HbaseClientMetrics.gauge("RequestManager:OutstandingCallbacks", new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return responseCallbacks.size();
            }
        });
    }

    public int registerResponseCallback(ResponseCallback callback) {
        int requestId = ids.getAndIncrement();
        responseCallbacks.put(requestId, callback);

        return requestId;
    }

    public Optional<ResponseCallback> retrieveCallback(int requestId) {
        return Optional.fromNullable(responseCallbacks.remove(requestId));
    }

    public void unregisterResponseCallback(int requestId) {
        responseCallbacks.remove(requestId);
    }

}
