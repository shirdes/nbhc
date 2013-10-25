package com.urbanairship.hbase.shc;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.urbanairship.hbase.environment.HBaseEnvironment;
import com.urbanairship.hbase.managers.Schemas;
import com.urbanairship.hbase.shc.dispatch.RequestManager;
import com.urbanairship.hbase.shc.dispatch.netty.DisconnectCallback;
import com.urbanairship.hbase.shc.dispatch.netty.HostChannelProvider;
import com.urbanairship.hbase.shc.dispatch.netty.NettyRegionServerDispatcher;
import com.urbanairship.hbase.shc.dispatch.netty.pipeline.HbaseClientPipelineFactory;
import com.urbanairship.hbase.shc.request.RequestSender;
import com.urbanairship.hbase.shc.scan.ScannerResultStream;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.*;

public class ClientTest {

    private static final String TABLE = "TEST";
    private static final byte[] FAMILY = "f".getBytes(Charsets.UTF_8);
    private static final byte[] COL = "c".getBytes(Charsets.UTF_8);

    private static HBaseEnvironment hbase;

    private static NioClientSocketChannelFactory channelFactory;
    private static HostChannelProvider channelProvider;
    private static HbaseClient client;

    @BeforeClass
    public static void setUp() throws Exception {
        hbase = new HBaseEnvironment(new MapConfiguration(ImmutableMap.of()));
        hbase.startAndWait();

        byte[][] splits = new byte[16][];
        for (int i = 0; i < 16; i++) {
            splits[i] = Integer.toString(i).getBytes(Charsets.UTF_8);
        }

        Schemas.run(hbase.getHadoopConfiguration(), ImmutableMap.of(
            Schemas.makeTableDescriptor(TABLE.getBytes(Charsets.UTF_8), Schemas.makeCfDesc(FAMILY, 1, StoreFile.BloomType.ROW, true)), splits
        ));

        ThreadFactory bossThreadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("Netty Boss Thread %d")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        e.printStackTrace();
                    }
                })
                .build();

        final ExecutorService boss = Executors.newFixedThreadPool(1, bossThreadFactory);

        ThreadFactory workerThreadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("Netty Worker Thread %d")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        e.printStackTrace();
                    }
                })
                .build();

        final ExecutorService workers = Executors.newFixedThreadPool(20, workerThreadFactory);

        channelFactory = new NioClientSocketChannelFactory(boss, workers);
        ClientBootstrap clientBootstrap = new ClientBootstrap(channelFactory);

        clientBootstrap.setOption("keepAlive", true);

        clientBootstrap.setOption("connectTimeoutMillis", 10000L);
        clientBootstrap.setOption("receiveBufferSize", 16777216);
        clientBootstrap.setOption("sendBufferSize", 16777216);
        clientBootstrap.setOption("tcpNoDelay", false);

        RequestManager requestManager = new RequestManager();
        DisconnectCallback disconnectCallback = new DisconnectCallback() {
            @Override
            public void disconnected(Channel channel) { }
        };

        clientBootstrap.setPipelineFactory(new HbaseClientPipelineFactory(requestManager, disconnectCallback));

        HConnection hconn = HConnectionManager.createConnection(hbase.getHadoopConfiguration());
        RegionOwnershipTopology topology = new HConnectionRegionOwnershipTopology(hconn);

        channelProvider = new HostChannelProvider(clientBootstrap);
        NettyRegionServerDispatcher dispatcher = new NettyRegionServerDispatcher(requestManager, channelProvider);

        RequestSender sender = new RequestSender(dispatcher);
        client = new HbaseClient(topology, sender, requestManager, 1);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        channelProvider.shutdown();
        channelFactory.releaseExternalResources();

        hbase.stop();
    }

    @Test
    public void testSingleCrud() throws Exception {
        String row = UUID.randomUUID().toString();
        String value = randomAlphabetic(5);

        Put put = new Put(Bytes.toBytes(row), System.currentTimeMillis());
        put.add(FAMILY, COL, Bytes.toBytes(value));

        Future<Void> putFuture = client.put(TABLE, put);
        putFuture.get();

        Get get = new Get(Bytes.toBytes(row));

        Future<Result> future = client.get(TABLE, get);

        Result result = future.get();

        assertTrue(result != null && !result.isEmpty());

        byte[] column = result.getValue(FAMILY, COL);
        assertTrue(column != null && column.length > 0);

        assertEquals(value, Bytes.toString(column));

        Delete delete = new Delete(Bytes.toBytes(row), System.currentTimeMillis() + 1L, null);
        Future<Void> deleteFuture = client.delete(TABLE, delete);
        deleteFuture.get();

        future = client.get(TABLE, get);
        result = future.get();

        assertTrue(result.isEmpty());
    }

    @Test
    public void testMulti() throws Exception {
        Map<String, String> entries = Maps.newHashMap();
        for (int i = 0; i < 100; i++) {
            entries.put(UUID.randomUUID().toString(), randomAlphanumeric(5));
        }

        List<Put> puts = Lists.newArrayList();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            Put put = new Put(Bytes.toBytes(entry.getKey()), System.currentTimeMillis());
            put.add(FAMILY, COL, Bytes.toBytes(entry.getValue()));

            puts.add(put);
        }

        ListenableFuture<Void> future = client.multiPut(TABLE, puts);
        future.get();

        List<Get> gets = Lists.newArrayList();
        for (String key : entries.keySet()) {
            gets.add(new Get(Bytes.toBytes(key)));
        }

        ListenableFuture<List<Result>> getFuture = client.multiGet(TABLE, gets);
        List<Result> results = getFuture.get();

        assertEquals(entries.size(), results.size());
        for (int i = 0; i < results.size(); i++) {
            Result result = results.get(i);

            assertArrayEquals(gets.get(i).getRow(), result.getRow());

            String key = Bytes.toString(result.getRow());
            assertEquals(entries.get(key), Bytes.toString(result.getValue(FAMILY, COL)));
        }

        Scan scan = new Scan();
        scan.addFamily(FAMILY);
        scan.setCaching(10);
        ScannerResultStream stream = client.getScannerStream(TABLE, scan);
        try {
            while (stream.hasNext()) {
                Result result = stream.next();

                String key = Bytes.toString(result.getRow());

                assertEquals(1, result.getFamilyMap(FAMILY).size());
                assertEquals(entries.get(key), Bytes.toString(result.getValue(FAMILY, COL)));
            }
        }
        finally {
            stream.close();
        }

        List<String> rows = Lists.newArrayList(entries.keySet());
        List<Delete> deletes = Lists.newArrayList();

        Set<String> removed = Sets.newHashSet();
        for (int i = 0; i < 3; i++) {
            int idx = RandomUtils.nextInt(rows.size());
            String row = rows.get(idx);
            deletes.add(new Delete(Bytes.toBytes(row), System.currentTimeMillis() + 1L, null));

            removed.add(row);
        }

        ListenableFuture<Void> deleteFuture = client.multiDelete(TABLE, deletes);
        deleteFuture.get();

        getFuture = client.multiGet(TABLE, gets);
        results = getFuture.get();

        assertEquals(entries.size(), results.size());
        for (int i = 0; i < results.size(); i++) {
            Result result = results.get(i);

            String key;
            if (result.isEmpty()) {
                Get correspondingGet = gets.get(i);
                key = Bytes.toString(correspondingGet.getRow());
            }
            else {
                key = Bytes.toString(result.getRow());
            }

            if (removed.contains(key)) {
                assertTrue(result.isEmpty());
            }
            else {
                assertEquals(entries.get(key), Bytes.toString(result.getValue(FAMILY, COL)));
            }
        }
    }
}
