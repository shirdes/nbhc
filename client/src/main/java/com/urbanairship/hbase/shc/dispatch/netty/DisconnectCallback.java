package com.urbanairship.hbase.shc.dispatch.netty;

import org.jboss.netty.channel.Channel;

public interface DisconnectCallback {

    void disconnected(Channel channel);

}
