package com.urbanairship.hbase.shc.operation;

import com.google.common.base.Function;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;

import java.lang.reflect.Method;

public class Operation<P, R> {

    private final HRegionLocation location;
    private final P param;
    private final Method targetMethod;
    private final Class<? extends VersionedProtocol> targetProtocol;
    private final Function<HbaseObjectWritable, R> responseParser;

    private final HostAndPort targetHost;

    public Operation(HRegionLocation location,
                     P param,
                     Method targetMethod,
                     Class<? extends VersionedProtocol> targetProtocol,
                     Function<HbaseObjectWritable, R> responseParser) {
        this.location = location;
        this.param = param;
        this.targetMethod = targetMethod;
        this.targetProtocol = targetProtocol;
        this.responseParser = responseParser;

        this.targetHost = HostAndPort.fromParts(location.getHostname(), location.getPort());
    }

    public HostAndPort getTargetHost() {
        return targetHost;
    }

    public Invocation getInvocation() {
        return new Invocation(targetMethod, targetProtocol, new Object[] {
                location.getRegionInfo().getRegionName(),
                param
        });
    }

    public Function<HbaseObjectWritable, R> getResponseValueParser() {
        return responseParser;
    }

}
