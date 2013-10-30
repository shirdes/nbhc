package com.urbanairship.hbase.shc.dispatch.netty;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.urbanairship.hbase.shc.dispatch.ConnectionHelloMessage;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.Channels;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class HostChannelProvider {

    private final AtomicBoolean active = new AtomicBoolean(true);

    private final ConcurrentMap<InetSocketAddress, ChannelPool> hostPools = new ConcurrentHashMap<InetSocketAddress, ChannelPool>();

    private final ClientBootstrap bootstrap;
    private final int maxConnectionsPerHost;

    public HostChannelProvider(ClientBootstrap bootstrap, int maxConnectionsPerHost) {
        this.bootstrap = bootstrap;
        this.maxConnectionsPerHost = maxConnectionsPerHost;
    }

    public Channel getChannel(HostAndPort host) {
        Preconditions.checkState(active.get());

        InetSocketAddress addr = new InetSocketAddress(host.getHostText(), host.getPort());

        ChannelPool pool = hostPools.get(addr);
        if (pool == null) {
            pool = new ChannelPool(addr, maxConnectionsPerHost);
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

    private final class ChannelPool {

        private final BlockingQueue<Channel> activeChannels = new LinkedBlockingQueue<Channel>();
        private final AtomicInteger activeChannelCount = new AtomicInteger(0);

        private final ConcurrentSkipListSet<Channel> killed = new ConcurrentSkipListSet<Channel>();

        private final AtomicBoolean poolActive = new AtomicBoolean(true);

        private final InetSocketAddress host;
        private final int maxChannels;

        private final Semaphore creationPermits;

        private ChannelPool(InetSocketAddress host, int maxChannels) {
            this.host = host;
            this.maxChannels = maxChannels;

            this.creationPermits = new Semaphore(maxChannels);
        }

        public Channel getChannel() {
            Preconditions.checkState(poolActive.get());

            Optional<Channel> channel;
            do {
                channel = procureChannel();
            } while (!channel.isPresent());

            return channel.get();
        }

        public void removeChannel(Channel channel) {
            if (!activeChannels.remove(channel)) {
                // another thread could have pulled the channel from the queue during it's retrieve operation
                // so we need to set a tombstone for it so that if it comes out of the queue again, it will be
                // ignored and not put back
                killed.add(channel);
            }

            creationPermits.release();
            activeChannelCount.decrementAndGet();
        }

        public void shutdown() {
            poolActive.set(false);
            for (Channel channel : activeChannels) {
                // TODO: should we wait to ensure it gets closed?
                Channels.close(channel);
            }

            activeChannels.clear();
            activeChannelCount.set(0);
        }

        private Optional<Channel> procureChannel() {
            Optional<Channel> channel = Optional.absent();
            if (activeChannelCount.get() < maxChannels) {
                channel = attemptChannelCreate();
            }

            return channel.isPresent() ? channel : getActiveChannel();
        }

        private Optional<Channel> attemptChannelCreate() {
            if (!creationPermits.tryAcquire()) {
                return Optional.absent();
            }

            Channel channel = createChannel(host);
            activeChannelCount.incrementAndGet();
            activeChannels.add(channel);

            return Optional.of(channel);
        }

        private Optional<Channel> getActiveChannel() {
            // TODO: expose to config
            Channel channel;
            try {
                channel = activeChannels.poll(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted waiting to retrieve active channel!");
            }

            if (killed.remove(channel)) {
                channel = null;
            }

            if (channel != null) {
                activeChannels.add(channel);
            }

            return Optional.fromNullable(channel);
        }
    }

}
