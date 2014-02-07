package org.wizbang.hbase.nbhc.dispatch;

import com.codahale.metrics.Gauge;
import com.google.common.base.Optional;
import org.wizbang.hbase.nbhc.HbaseClientMetrics;
import org.wizbang.hbase.nbhc.response.RequestResponseController;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestManager {

    private final AtomicInteger ids = new AtomicInteger(0);

    private final ConcurrentMap<Integer, RequestResponseController> controllers = new ConcurrentHashMap<Integer, RequestResponseController>();

    public RequestManager() {
        HbaseClientMetrics.gauge("RequestManager:OutstandingCallbacks", new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return controllers.size();
            }
        });
    }

    public int registerController(RequestResponseController controller) {
        int requestId = ids.getAndIncrement();
        controllers.put(requestId, controller);

        return requestId;
    }

    public Optional<RequestResponseController> retrieveCallback(int requestId) {
        return Optional.fromNullable(controllers.remove(requestId));
    }

    public void unregisterResponseCallback(int requestId) {
        controllers.remove(requestId);
    }

}
