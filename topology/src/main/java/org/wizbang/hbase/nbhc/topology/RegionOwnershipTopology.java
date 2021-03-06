package org.wizbang.hbase.nbhc.topology;

import org.apache.hadoop.hbase.HRegionLocation;

public interface RegionOwnershipTopology {

    HRegionLocation getRegionServer(String table, byte[] targetRow);

    HRegionLocation getRegionServerNoCache(String table, byte[] targetRow);

}
