package org.wizbang.hbase.nbhc.dispatch;

import org.apache.hadoop.hbase.ipc.Invocation;

public final class Request {

    private final int requestId;
    private final Invocation invocation;

    public Request(int requestId, Invocation invocation) {
        this.requestId = requestId;
        this.invocation = invocation;
    }

    public int getRequestId() {
        return requestId;
    }

    public Invocation getInvocation() {
        return invocation;
    }

    // TODO: toString()
}
