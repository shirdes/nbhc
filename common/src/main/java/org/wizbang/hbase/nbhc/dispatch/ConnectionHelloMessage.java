package org.wizbang.hbase.nbhc.dispatch;

import com.google.common.base.Charsets;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HRegionInterface;

import java.nio.ByteBuffer;

public final class ConnectionHelloMessage {

    public static final ConnectionHelloMessage INSTANCE = new ConnectionHelloMessage();

    private static final byte[] MAGIC = HBaseServer.HEADER.array();
    private static final byte VERSION = HBaseServer.CURRENT_VERSION;

    private static final byte[] PROTOCOL = HRegionInterface.class.getName().getBytes(Charsets.UTF_8);
    private static final byte[] PROTOCOL_BINARY_FORMAT;
    static {
        byte[] bytes = new byte[1 + PROTOCOL.length];
        ByteBuffer.wrap(bytes)
                .put((byte) PROTOCOL.length)
                .put(PROTOCOL);
        PROTOCOL_BINARY_FORMAT = bytes;
    }

    private ConnectionHelloMessage() { }

    public byte[] getMagic() {
        return MAGIC;
    }

    public byte getVersion() {
        return VERSION;
    }

    public byte[] getProtocol() {
        return PROTOCOL_BINARY_FORMAT;
    }
}
