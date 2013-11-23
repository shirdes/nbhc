package org.wizbang.hbase.nbhc;

import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.request.OperationFutureSupplier;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wizbang.hbase.nbhc.topology.*;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TopologyTest {

    private static final String TABLE = "TOPOLOGY.TEST";

    private static RegionServerDispatcherService dispatcherService;
    private static CuratorFramework curator;
    private static ZookeeperHbaseClusterTopology clusterTopology;

    private static MetaTable metaTable;
    private static HConnection hconn;

    private static Configuration config;

    @BeforeClass
    public static void setUp() throws Exception {
        config = HBaseConfiguration.create();
        config.set("zookeeper.znode.parent", "/hbase");
        config.set("hbase.zookeeper.quorum", "localhost:2181");

        try {
            hconn = HConnectionManager.createConnection(config);
        }
        catch (Exception e) {
            throw new RuntimeException("Error creating hbase connection", e);
        }

        RequestManager requestManager = new RequestManager();

        dispatcherService = NettyDispatcherFactory.create(requestManager);
        dispatcherService.startAndWait();

        RequestSender sender = new RequestSender(dispatcherService.getDispatcher());

        OperationFutureSupplier futureSupplier = new OperationFutureSupplier(requestManager);

        TopologyOperationsClient operationsClient = new TopologyOperationsClient(sender, futureSupplier, 3);
        MetaTableLookupSource metaSource = new MetaTableLookupSource(operationsClient, TopologyUtil.INSTACE);

        Supplier<LocationCache> cacheSupplier = new Supplier<LocationCache>() {
            @Override
            public LocationCache get() {
                return new LocationCache();
            }
        };

        curator = CuratorFrameworkFactory.newClient("localhost:2181/hbase", new RetryNTimes(5, 100));
        curator.start();

        clusterTopology = new ZookeeperHbaseClusterTopology(curator);
        clusterTopology.startAndWait();

        RootTableLookupSource rootTableLookupSource = new RootTableLookupSource(clusterTopology, operationsClient, TopologyUtil.INSTACE);
        RootTable rootTable = new RootTable(rootTableLookupSource, new LocationCache());

        metaTable = new MetaTable(metaSource, cacheSupplier, rootTable);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        dispatcherService.stopAndWait();
        clusterTopology.stopAndWait();
        curator.close();
    }

    @Test
    public void testLocations() throws Exception {
        byte[][] splits = new byte[16][];
        for (int i = 0; i < 16; i++) {
            splits[i] = new byte[]{(byte)i};
        }

        HBaseAdmin admin = new HBaseAdmin(hconn);
        if (!admin.tableExists(TABLE)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("F");
            columnDescriptor.setBloomFilterType(StoreFile.BloomType.ROW);
            columnDescriptor.setCompressionType(Compression.Algorithm.GZ);
            columnDescriptor.setMaxVersions(1);
            columnDescriptor.setTimeToLive(Integer.MAX_VALUE);
            tableDescriptor.addFamily(columnDescriptor);

            admin.createTable(tableDescriptor, splits);
        }

        Map<String, HRegionLocation> expected = Maps.newHashMap();
        for (int i = 0; i < 50; i++) {
            String id = UUID.randomUUID().toString();
            byte[] key = getKey(id);

            HRegionLocation location = hconn.getRegionLocation(TABLE.getBytes(Charsets.UTF_8), key, true);
            expected.put(id, location);
        }

        for (Map.Entry<String, HRegionLocation> entry : expected.entrySet()) {
            byte[] key = getKey(entry.getKey());

            HRegionLocation location = metaTable.getRegionServerNoCache(TABLE, key);

            assertEquals(String.format("Expected %s but got %s for id %s", entry.getValue().toString(), location.toString(), entry.getKey()), entry.getValue(), location);
        }
    }

    private byte[] getKey(String id) {
        int digit = Character.digit(id.charAt(0), 16);

        byte[] bytes = id.getBytes();
        byte[] key = new byte[2 + bytes.length];
        ByteBuffer.wrap(key)
                .put((byte)digit)
                .put((byte)':')
                .put(bytes);

        return key;
    }

}
