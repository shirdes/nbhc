package org.wizbang.hbase.nbhc;

import com.codahale.metrics.Gauge;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.wizbang.hbase.nbhc.dispatch.RegionServerDispatcher;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.netty.DisconnectCallback;
import org.wizbang.hbase.nbhc.netty.HostChannelProvider;
import org.wizbang.hbase.nbhc.netty.NettyRegionServerDispatcher;
import org.wizbang.hbase.nbhc.netty.pipeline.HbaseClientPipelineFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NettyDispatcherFactory {

    public static RegionServerDispatcherService create(RequestManager requestManager) {
        return new Service(requestManager);
    }

    private static final class Service extends AbstractIdleService implements RegionServerDispatcherService {

        private final RequestManager requestManager;

        private NettyRegionServerDispatcher dispatcher;

        private NioClientSocketChannelFactory channelFactory;
        private HostChannelProvider channelProvider;

        private Service(RequestManager requestManager) {
            this.requestManager = requestManager;
        }

        @Override
        protected void startUp() throws Exception {
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

            ExecutorService boss = Executors.newFixedThreadPool(1, bossThreadFactory);

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

            // TODO: expose to config
            final LinkedBlockingQueue<Runnable> nettyWorkerQueue = new LinkedBlockingQueue<Runnable>();
            ExecutorService workers = new ThreadPoolExecutor(200, 200,
                    0L, TimeUnit.MILLISECONDS,
                    nettyWorkerQueue,
                    workerThreadFactory);

            HbaseClientMetrics.gauge("Netty:WorkerQueueSize", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return nettyWorkerQueue.size();
                }
            });

            channelFactory = new NioClientSocketChannelFactory(boss, workers);
            ClientBootstrap clientBootstrap = new ClientBootstrap(channelFactory);

            clientBootstrap.setOption("keepAlive", true);

            clientBootstrap.setOption("connectTimeoutMillis", 10000L);
            clientBootstrap.setOption("receiveBufferSize", 16777216);
            clientBootstrap.setOption("sendBufferSize", 16777216);
            clientBootstrap.setOption("tcpNoDelay", false);

            channelProvider = new HostChannelProvider(clientBootstrap, 10);

            DisconnectCallback disconnectCallback = new DisconnectCallback() {
                @Override
                public void disconnected(Channel channel) {
                    channelProvider.removeChannel(channel);
                }
            };

            clientBootstrap.setPipelineFactory(new HbaseClientPipelineFactory(requestManager, disconnectCallback, channelProvider));

            dispatcher = new NettyRegionServerDispatcher(requestManager, channelProvider);
        }

        @Override
        protected void shutDown() throws Exception {
            channelProvider.shutdown();
            channelFactory.releaseExternalResources();
        }

        @Override
        public RegionServerDispatcher getDispatcher() {
            Preconditions.checkState(state() == State.RUNNING);
            return dispatcher;
        }
    }
}
