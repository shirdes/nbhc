package com.urbanairship.hbase.shc.operation;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.hbase.ipc.Invocation;

public final class Operation {

    private final HostAndPort targetHost;
    private final Invocation invocation;

    public Operation(HostAndPort targetHost, Invocation invocation) {
        this.targetHost = targetHost;
        this.invocation = invocation;
    }

    public HostAndPort getTargetHost() {
        return targetHost;
    }

    public Invocation getInvocation() {
        return invocation;
    }
}
