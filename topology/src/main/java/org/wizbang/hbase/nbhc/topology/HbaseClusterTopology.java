package org.wizbang.hbase.nbhc.topology;

import org.apache.hadoop.hbase.ServerName;

public interface HbaseClusterTopology {

    ServerName getMasterServer();

    ServerName getRootRegionServer();
}
