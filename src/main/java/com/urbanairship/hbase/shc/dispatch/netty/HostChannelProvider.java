package com.urbanairship.hbase.shc.dispatch.netty;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.urbanairship.hbase.shc.dispatch.ConnectionHelloMessage;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HostChannelProvider {

    private final AtomicBoolean active = new AtomicBoolean(true);

    private final ConcurrentMap<InetSocketAddress, ChannelPool> hostPools = new ConcurrentHashMap<InetSocketAddress, ChannelPool>();

    private final ClientBootstrap bootstrap;
    private final int maxConnectionsPerHost;

    private final Function<InetSocketAddress, Channel> channelCreator = new Function<InetSocketAddress, Channel>() {
        @Override
        public Channel apply(InetSocketAddress addr) {
            return createChannel(addr);
        }
    };

    public HostChannelProvider(ClientBootstrap bootstrap, int maxConnectionsPerHost) {
        this.bootstrap = bootstrap;
        this.maxConnectionsPerHost = maxConnectionsPerHost;
    }

    public Channel getChannel(HostAndPort host) {
        Preconditions.checkState(active.get());

        InetSocketAddress addr = new InetSocketAddress(host.getHostText(), host.getPort());

        ChannelPool pool = hostPools.get(addr);
        if (pool == null) {
            pool = new ChannelPool(addr, channelCreator, maxConnectionsPerHost);
            ChannelPool had = hostPools.putIfAbsent(addr, pool);
            if (had != null) {
                pool = had;
            }
        }

        return pool.getChannel();
    }

    public void removeChannel(Channel channel) {
        if (!active.get()) {
            return;
        }

        SocketAddress remote = channel.getRemoteAddress();
        if (!(remote instanceof InetSocketAddress)) {
            return;
        }

        InetSocketAddress addr = (InetSocketAddress) remote;
        ChannelPool pool = hostPools.get(addr);
        pool.removeChannel(channel);
    }

    private Channel createChannel(InetSocketAddress host) {

        ChannelFuture future = bootstrap.connect(host);
        try {
            // TODO: expose to config
            if (!future.await(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("Unable to connect to host " + host);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for connection to host " + host, e);
        }

        Channel channel = future.getChannel();
        if (!channel.isConnected()) {
            throw new RuntimeException("Channel connection not established to host " + host);
        }

        if (!sendHelloMessage(channel)) {
            throw new RuntimeException("Failed to send hello message on newly created channel");
        }

        return channel;
    }

    private boolean sendHelloMessage(Channel channel) {
        ChannelFuture future = channel.write(ConnectionHelloMessage.INSTANCE);
        try {
            // TODO: expose to config
            if (!future.await(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("Unable to send hello message on channel");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting to send hello message on channel", e);
        }

        return future.isSuccess();
    }

    public void shutdown() {
        active.set(false);

        // TODO: technically there is a race condition with this and the getChannel method since a thread could
        // TODO: get past the active check just before we set it to false.  Right now my thought is that if the caller
        // TODO: doesn't ensure that their own threads that make requests are done before calling our shutdown, that's
        // TODO: on them to deal with the potential for the race.
        for (InetSocketAddress host : hostPools.keySet()) {
            hostPools.remove(host).shutdown();
        }
    }

}
