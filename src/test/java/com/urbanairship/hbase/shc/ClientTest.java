package com.urbanairship.hbase.shc;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.urbanairship.hbase.environment.HBaseEnvironment;
import com.urbanairship.hbase.managers.Schemas;
import com.urbanairship.hbase.shc.dispatch.RequestManager;
import com.urbanairship.hbase.shc.dispatch.netty.DisconnectCallback;
import com.urbanairship.hbase.shc.dispatch.netty.HostChannelProvider;
import com.urbanairship.hbase.shc.dispatch.netty.NettyRegionServerDispatcher;
import com.urbanairship.hbase.shc.dispatch.netty.pipeline.HbaseClientPipelineFactory;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

        Schemas.run(hbase.getHadoopConfiguration(), ImmutableMap.of(
                Schemas.makeTableDescriptor(TABLE.getBytes(Charsets.UTF_8), Schemas.makeCfDesc(FAMILY, 1, StoreFile.BloomType.ROW, true)), new byte[0][]
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

        client = new HbaseClient(dispatcher, topology);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        channelProvider.shutdown();
        channelFactory.releaseExternalResources();

        hbase.stop();
    }

    @Test
    public void testName() throws Exception {
        String row = RandomStringUtils.randomAlphabetic(10);
        String value = RandomStringUtils.randomAlphabetic(5);

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
}
