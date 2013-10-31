package com.urbanairship.hbase.shc;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.urbanairship.hbase.shc.dispatch.RequestManager;
import com.urbanairship.hbase.shc.dispatch.netty.DisconnectCallback;
import com.urbanairship.hbase.shc.dispatch.netty.HostChannelProvider;
import com.urbanairship.hbase.shc.dispatch.netty.NettyRegionServerDispatcher;
import com.urbanairship.hbase.shc.dispatch.netty.pipeline.HbaseClientPipelineFactory;
import com.urbanairship.hbase.shc.request.RequestSender;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public final class HbaseClientFactory {

    public static HbaseClientService create(Configuration hbaseConfig) {
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

        final ExecutorService workers = Executors.newFixedThreadPool(200, workerThreadFactory);

        NioClientSocketChannelFactory channelFactory = new NioClientSocketChannelFactory(boss, workers);
        ClientBootstrap clientBootstrap = new ClientBootstrap(channelFactory);

        clientBootstrap.setOption("keepAlive", true);

        clientBootstrap.setOption("connectTimeoutMillis", 10000L);
        clientBootstrap.setOption("receiveBufferSize", 16777216);
        clientBootstrap.setOption("sendBufferSize", 16777216);
        clientBootstrap.setOption("tcpNoDelay", false);

        final HostChannelProvider channelProvider = new HostChannelProvider(clientBootstrap, 10);

        RequestManager requestManager = new RequestManager();
        DisconnectCallback disconnectCallback = new DisconnectCallback() {
            @Override
            public void disconnected(Channel channel) {
                channelProvider.removeChannel(channel);
            }
        };

        clientBootstrap.setPipelineFactory(new HbaseClientPipelineFactory(requestManager, disconnectCallback, channelProvider));

        HConnection hconn;
        try {
            hconn = HConnectionManager.createConnection(hbaseConfig);
            hconn.getRegionLocation(HConstants.ROOT_TABLE_NAME, HConstants.EMPTY_BYTE_ARRAY, false);
        }
        catch (Exception e) {
            throw new RuntimeException("Error creating hbase connection", e);
        }

        RegionOwnershipTopology topology = new HConnectionRegionOwnershipTopology(hconn);

        NettyRegionServerDispatcher dispatcher = new NettyRegionServerDispatcher(requestManager, channelProvider);

        RequestSender sender = new RequestSender(dispatcher);

        HbaseClient client = new HbaseClient(topology, sender, requestManager, 1);

        return new ClientService(channelFactory, channelProvider, client);
    }

    private static final class ClientService extends AbstractIdleService implements HbaseClientService {

        private final NioClientSocketChannelFactory channelFactory;
        private final HostChannelProvider channelProvider;
        private final HbaseClient client;

        private ClientService(NioClientSocketChannelFactory channelFactory, HostChannelProvider channelProvider, HbaseClient client) {
            this.channelFactory = channelFactory;
            this.channelProvider = channelProvider;
            this.client = client;
        }

        @Override
        protected void startUp() throws Exception { }

        @Override
        protected void shutDown() throws Exception {
            channelProvider.shutdown();
            channelFactory.releaseExternalResources();
        }

        @Override
        public HbaseClient getClient() {
            return client;
        }
    }
}
