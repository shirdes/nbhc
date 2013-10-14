package com.urbanairship.hbase.shc.dispatch.netty;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.net.HostAndPort;
import com.urbanairship.hbase.shc.dispatch.ConnectionHelloMessage;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class HostChannelProvider {

    private final ConcurrentMap<HostAndPort, Channel> hostChannels = new ConcurrentHashMap<HostAndPort, Channel>();

    private final Interner<HostAndPort> interner = Interners.newWeakInterner();

    private final ClientBootstrap bootstrap;

    public HostChannelProvider(ClientBootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }

    public Channel getChannel(HostAndPort host) {
        Channel channel = hostChannels.get(host);
        if (channel != null) {
            return channel;
        }

        synchronized (interner.intern(host)) {
            channel = hostChannels.get(host);
            if (channel != null) {
                return channel;
            }

            channel = createChannel(host);
            hostChannels.put(host, channel);
        }

        return channel;
    }

    private Channel createChannel(HostAndPort host) {

        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host.getHostText(), host.getPort()));
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
        // TODO: need to find a better way to do this
        Set<HostAndPort> hosts = ImmutableSet.copyOf(hostChannels.keySet());
        for (HostAndPort host : hosts) {
            Channel channel = hostChannels.remove(host);

            ChannelFuture future = channel.close();
            future.awaitUninterruptibly();
        }
    }

}
