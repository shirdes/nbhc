package com.urbanairship.hbase.shc;

import com.urbanairship.hbase.shc.request.RequestSender;
import com.urbanairship.hbase.shc.topology.MetaTable;
import com.urbanairship.hbase.shc.topology.MetaTableLookupSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.junit.Before;
import org.junit.BeforeClass;

public class TopologyTest {

    private static MetaTable metaTable;
    private static HConnection hConnection;

//    @BeforeClass
//    public static void setUp() throws Exception {
//        Configuration config = HBaseConfiguration.create();
//        config.set("zookeeper.znode.parent", "/hbase");
//        config.set("hbase.zookeeper.quorum", "localhost:2181");
//
//        HConnection hconn;
//        try {
//            hconn = HConnectionManager.createConnection(config);
//            hconn.getRegionLocation(HConstants.ROOT_TABLE_NAME, HConstants.EMPTY_BYTE_ARRAY, false);
//        }
//        catch (Exception e) {
//            throw new RuntimeException("Error creating hbase connection", e);
//        }
//
//        new RequestSender()
//        new TopologyOperationsClient()
//        new MetaTableLookupSource()
//        metaTable = new MetaTable()
//
//    }
}
