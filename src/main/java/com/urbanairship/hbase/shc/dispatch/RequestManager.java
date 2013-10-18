package com.urbanairship.hbase.shc.dispatch;

import com.google.common.base.Optional;
import com.urbanairship.hbase.shc.response.ResponseCallback;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestManager {

    private final AtomicInteger ids = new AtomicInteger(0);

    private final ConcurrentMap<Integer, ResponseCallback> responseCallbacks = new ConcurrentHashMap<Integer, ResponseCallback>();

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
