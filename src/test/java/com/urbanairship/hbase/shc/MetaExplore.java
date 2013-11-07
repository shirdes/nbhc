package com.urbanairship.hbase.shc;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.junit.Test;

public class MetaExplore {

    @Test
    public void testMeta() throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("zookeeper.znode.parent", "/hbase");
        config.set("hbase.zookeeper.quorum", "localhost:2181");

        HConnection conn = HConnectionManager.createConnection(config);
        conn.getRegionLocation("TEST".getBytes(Charsets.UTF_8), "1".getBytes(), false);
    }
}
