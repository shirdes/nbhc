package org.wizbang.hbase.nbhc.topology;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ZookeeperHbaseClusterTopology extends AbstractIdleService implements HbaseClusterTopology {

    private static final Logger log = LogManager.getLogger(ZookeeperHbaseClusterTopology.class);

    private final CuratorFramework zk;

    private NodeCache masterServerNode;
    private NodeCache rootRegionServerNode;

    public ZookeeperHbaseClusterTopology(CuratorFramework zk) {
        this.zk = zk;
    }

    @Override
    protected void startUp() throws Exception {
        masterServerNode = new NodeCache(zk, "/master");
        masterServerNode.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                log.info("The master server changed!");
            }
        });

        masterServerNode.start(true);

        rootRegionServerNode = new NodeCache(zk, "/root-region-server");
        // TODO: really??  terrible...
        rootRegionServerNode.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                log.info("The root region server changed!");
            }
        });

        rootRegionServerNode.start(true);
    }

    @Override
    protected void shutDown() throws Exception {
        rootRegionServerNode.close();
        masterServerNode.close();
    }

    @Override
    public ServerName getMasterServer() {
        ChildData data = masterServerNode.getCurrentData();
        Preconditions.checkNotNull(data, "No data in the master server node");

        byte[] raw = data.getData();
        raw = removeMetaData(raw);
        return ServerName.parseVersionedServerName(raw);
    }

    @Override
    public ServerName getRootRegionServer() {
        ChildData data = rootRegionServerNode.getCurrentData();
        Preconditions.checkNotNull(data, "No data in root region server node!");

        byte[] raw = data.getData();
        raw = removeMetaData(raw);
        String rawString = new String(raw, Charsets.UTF_8);
        // TODO: seems to be handling some old condition that may not even be possible anymore?
        if (rawString.contains(ServerName.SERVERNAME_SEPARATOR)) {
            return ServerName.parseServerName(rawString);
        }

        HostAndPort hostAndPort = HostAndPort.fromString(rawString);
        return new ServerName(hostAndPort.getHostText(), hostAndPort.getPort(), -1L);
    }

    /**
     * Copied from RecoverableZooKeeper in Hbase codez
     */
    private static final byte MAGIC =(byte) 0XFF;
    private static final int MAGIC_SIZE = Bytes.SIZEOF_BYTE;
    private static final int ID_LENGTH_OFFSET = MAGIC_SIZE;
    private static final int ID_LENGTH_SIZE =  Bytes.SIZEOF_INT;

    private byte[] removeMetaData(byte[] data) {
        if(data == null || data.length == 0) {
            return data;
        }
        // check the magic data; to be backward compatible
        byte magic = data[0];
        if(magic != MAGIC) {
            return data;
        }

        int idLength = Bytes.toInt(data, ID_LENGTH_OFFSET);
        int dataLength = data.length-MAGIC_SIZE-ID_LENGTH_SIZE-idLength;
        int dataOffset = MAGIC_SIZE+ID_LENGTH_SIZE+idLength;

        byte[] newData = new byte[dataLength];
        System.arraycopy(data, dataOffset, newData, 0, dataLength);

        return newData;

    }
}
