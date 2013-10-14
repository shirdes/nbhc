package com.urbanairship.hbase.shc;

import org.apache.hadoop.hbase.ServerName;

public interface HbaseClusterTopology {

    ServerName getMasterServer();

    ServerName getRootRegionServer();
}
