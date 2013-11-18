package com.urbanairship.hbase.shc.topology;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Result;

public interface TopologyOperations {

    Optional<Result> getRowOrBefore(byte[] row, Supplier<HRegionLocation> locationSupplier);

}
