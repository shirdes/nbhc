package org.wizbang.hbase.nbhc.netty;

import org.jboss.netty.channel.Channel;

public interface DisconnectCallback {

    void disconnected(Channel channel);

}
