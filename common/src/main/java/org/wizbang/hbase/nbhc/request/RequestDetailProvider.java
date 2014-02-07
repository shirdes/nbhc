package org.wizbang.hbase.nbhc.request;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ipc.Invocation;

public interface RequestDetailProvider {

    HRegionLocation getLocation();

    HRegionLocation getRetryLocation();

    Invocation getInvocation(HRegionLocation targetLocation);

}
