package org.wizbang.hbase.nbhc.dispatch;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.ipc.Invocation;

public final class Request {

    private final int requestId;
    private final Invocation invocation;

    public Request(int requestId, Invocation invocation) {
        this.requestId = requestId;
        this.invocation = Preconditions.checkNotNull(invocation);
    }

    public int getRequestId() {
        return requestId;
    }

    public Invocation getInvocation() {
        return invocation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Request request = (Request) o;

        if (requestId != request.requestId) return false;
        if (!invocation.equals(request.invocation)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = requestId;
        result = 31 * result + invocation.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("requestId", requestId)
                .add("invocation", invocation)
                .toString();
    }
}
