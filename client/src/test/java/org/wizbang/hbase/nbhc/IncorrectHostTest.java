package org.wizbang.hbase.nbhc;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang.math.RandomUtils;
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
import org.wizbang.hbase.nbhc.request.SingleActionRequestInitiator;
import org.wizbang.hbase.nbhc.request.multi.MultiActionRequestInitiator;
import org.wizbang.hbase.nbhc.request.multi.MultiActionResponseParser;
import org.wizbang.hbase.nbhc.request.scan.ScannerInitiator;
import org.wizbang.hbase.nbhc.topology.HbaseMetaService;
import org.wizbang.hbase.nbhc.topology.HbaseMetaServiceFactory;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IncorrectHostTest {

    private static final String TABLE = "TEST";
    private static final byte[] FAMILY = "f".getBytes(Charsets.UTF_8);
    private static final byte[] COL = "c".getBytes(Charsets.UTF_8);

    // Update this map with hosts for the instance the client is running against
    private static final Map<String, String> HOST_SWAP = ImmutableMap.of(
            "wizbang", "rs1",
            "rs1", "wizbang"
    );

    private static RequestManager requestManager;
    private static RequestSender sender;
    private static SchedulerWithWorkersRetryExecutor retryExecutor;
    private static HbaseClientConfiguration clientConfig;

    private static RegionServerDispatcherService dispatcherService;
    private static HbaseMetaService metaService;

    private static SingleActionRequestInitiator singleActionRequestInitiator;
    private static MultiActionRequestInitiator multiActionRequestInitiator;
    private static ScannerInitiator scannerInitiator;

    private static ExecutorService workerPool;

    @BeforeClass
    public static void setUp() throws Exception {
        requestManager = new RequestManager();

        dispatcherService = NettyDispatcherFactory.create(requestManager);
        dispatcherService.startAsync().awaitRunning();

        sender = new RequestSender(requestManager, dispatcherService.getDispatcher());

        clientConfig = new HbaseClientConfiguration();
        workerPool = Executors.newCachedThreadPool();

        retryExecutor = new SchedulerWithWorkersRetryExecutor(workerPool, clientConfig);
        retryExecutor.startAsync().awaitRunning();

        singleActionRequestInitiator = new SingleActionRequestInitiator(sender, workerPool, retryExecutor, requestManager,
                RemoteErrorUtil.INSTANCE, clientConfig);

        metaService = HbaseMetaServiceFactory.create(singleActionRequestInitiator, clientConfig);
        metaService.startAsync().awaitRunning();

        multiActionRequestInitiator = new MultiActionRequestInitiator(sender, workerPool, retryExecutor, requestManager,
                metaService.getTopology(), MultiActionResponseParser.INSTANCE);

        scannerInitiator = new ScannerInitiator(metaService.getTopology(), singleActionRequestInitiator, clientConfig);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        retryExecutor.stopAsync().awaitTerminated();
        metaService.stopAsync().awaitTerminated();
        dispatcherService.stopAsync().awaitTerminated();
        workerPool.shutdownNow();
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

        HbaseClient badClient = new HbaseClientImpl(badTopology, singleActionRequestInitiator, multiActionRequestInitiator,
                scannerInitiator);

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

    @Test
    public void testIncorrectHostMultiAction() throws Exception {
        final RegionOwnershipTopology topology = metaService.getTopology();

        RegionOwnershipTopology badTopology = new RegionOwnershipTopology() {
            @Override
            public HRegionLocation getRegionServer(String table, byte[] targetRow) {
                HRegionLocation actual = topology.getRegionServer(table, targetRow);
                return RandomUtils.nextInt() % 2 == 0
                        ? actual
                        : new HRegionLocation(actual.getRegionInfo(), HOST_SWAP.get(actual.getHostname()), actual.getPort());
            }

            @Override
            public HRegionLocation getRegionServerNoCache(String table, byte[] targetRow) {
                return topology.getRegionServerNoCache(table, targetRow);
            }
        };

        HbaseClient client = new HbaseClientImpl(badTopology, singleActionRequestInitiator, multiActionRequestInitiator,
                scannerInitiator);

        Map<String, String> values = Maps.newHashMap();
        for (int i = 0; i < 25; i++) {
            values.put(randomAlphanumeric(10), randomAlphabetic(8));
        }

        ImmutableList.Builder<Put> puts = ImmutableList.builder();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            Put put = new Put(Bytes.toBytes(entry.getKey()));
            put.add(FAMILY, COL, Bytes.toBytes(entry.getValue()));
            puts.add(put);
        }

        ListenableFuture<Void> future = client.multiPut(TABLE, puts.build());
        future.get(60, TimeUnit.SECONDS);

        ImmutableList.Builder<Get> gets = ImmutableList.builder();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            gets.add(new Get(Bytes.toBytes(entry.getKey())));
        }

        ListenableFuture<ImmutableList<Result>> getFuture = client.multiGet(TABLE, gets.build());
        ImmutableList<Result> results = getFuture.get(60, TimeUnit.SECONDS);

        Map<String, String> retrieved = Maps.newHashMap();
        for (Result result : results) {
            retrieved.put(Bytes.toString(result.getRow()), Bytes.toString(result.getValue(FAMILY, COL)));
        }

        assertEquals(values, retrieved);
    }
}
