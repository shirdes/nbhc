package org.wizbang.hbase.nbhc.dispatch;

import org.wizbang.hbase.nbhc.Operation;
import org.apache.hadoop.hbase.ipc.Invocation;

public final class Request {

    private final int requestId;
    private final Operation operation;

    public Request(int requestId, Operation operation) {
        this.requestId = requestId;
        this.operation = operation;
    }

    public int getRequestId() {
        return requestId;
    }

    public Invocation getInvocation() {
        return operation.getInvocation();
    }

    // TODO: toString()
}