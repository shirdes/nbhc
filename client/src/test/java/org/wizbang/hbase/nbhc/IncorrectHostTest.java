package org.wizbang.hbase.nbhc;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.netty.NettyDispatcherFactory;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.topology.HbaseMetaService;
import org.wizbang.hbase.nbhc.topology.HbaseMetaServiceFactory;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IncorrectHostTest {

    private static final String TABLE = "TEST";
    private static final byte[] FAMILY = "f".getBytes(Charsets.UTF_8);
    private static final byte[] COL = "c".getBytes(Charsets.UTF_8);

    // Update this map with hosts for the instance the client is running against
    private static final Map<String, String> HOST_SWAP = ImmutableMap.of(
            "wizbang", "linux-vm",
            "linux-vm", "wizbang"
    );

    private static RequestManager requestManager;
    private static RequestSender sender;

    private static RegionServerDispatcherService dispatcherService;
    private static HbaseMetaService metaService;

    @BeforeClass
    public static void setUp() throws Exception {
        requestManager = new RequestManager();

        dispatcherService = NettyDispatcherFactory.create(requestManager);
        dispatcherService.startAndWait();

        sender = new RequestSender(requestManager, dispatcherService.getDispatcher());

        metaService = HbaseMetaServiceFactory.create(requestManager, sender);
        metaService.startAndWait();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        metaService.stopAndWait();
        dispatcherService.stopAndWait();
    }

    @Test
    public void testIncorrectHost() throws Exception {
        final RegionOwnershipTopology topology = metaService.getTopology();

        RegionOwnershipTopology badTopology = new RegionOwnershipTopology() {
            @Override
            public HRegionLocation getRegionServer(String table, byte[] targetRow) {
                HRegionLocation actual = topology.getRegionServer(table, targetRow);
                return new HRegionLocation(actual.getRegionInfo(), HOST_SWAP.get(actual.getHostname()), actual.getPort());
            }

            @Override
            public HRegionLocation getRegionServerNoCache(String table, byte[] targetRow) {
                return topology.getRegionServerNoCache(table, targetRow);
            }
        };

        HbaseClient badClient = new HbaseClient(badTopology, sender, requestManager, 3);

        byte[] row = Bytes.toBytes(UUID.randomUUID().toString());
        Put put = new Put(row);
        put.add(FAMILY, COL, Bytes.toBytes("blah"));

        badClient.put(TABLE, put).get();

        ListenableFuture<Result> future = badClient.get(TABLE, new Get(row));
        Result result = future.get();

        assertFalse(result.isEmpty());

        byte[] v = result.getValue(FAMILY, COL);
        assertEquals("blah", Bytes.toString(v));
    }
}
