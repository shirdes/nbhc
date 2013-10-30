package com.urbanairship.hbase.shc.dispatch.netty;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;

import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ChannelPool {

    private final BlockingQueue<Channel> activeChannels = new LinkedBlockingQueue<Channel>();
    private final AtomicInteger activeChannelCount = new AtomicInteger(0);

    private final ConcurrentSkipListSet<Channel> killed = new ConcurrentSkipListSet<Channel>();

    private final AtomicBoolean poolActive = new AtomicBoolean(true);

    private final InetSocketAddress host;
    private final Function<InetSocketAddress, Channel> channelCreator;
    private final int maxChannels;

    private final Semaphore creationPermits;

    public ChannelPool(InetSocketAddress host,
                       Function<InetSocketAddress, Channel> channelCreator,
                       int maxChannels) {
        this.host = host;
        this.channelCreator = channelCreator;
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

        Channel channel = channelCreator.apply(host);
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

        if (channel != null && killed.remove(channel)) {
            channel = null;
        }

        if (channel != null) {
            activeChannels.add(channel);
        }

        return Optional.fromNullable(channel);
    }
}
