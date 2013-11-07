package com.urbanairship.hbase.shc.topology;

import org.apache.hadoop.hbase.ServerName;

public interface HbaseClusterTopology {

    ServerName getMasterServer();

    ServerName getRootRegionServer();
}
