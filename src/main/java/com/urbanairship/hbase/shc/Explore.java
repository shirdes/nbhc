package com.urbanairship.hbase.shc;

import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;

public class Explore {

    public static void main(String[] args) {
//        CuratorFramework zk = CuratorFrameworkFactory.newClient("s0199.stag.urbanairship.com:2181/hbase-all", new RetryNTimes(5, 1000));
//        zk.start();
//
//        ZookeeperHbaseClusterTopology topology = new ZookeeperHbaseClusterTopology(zk);
//        topology.startAndWait();
//
//        System.out.println(topology.getMasterServer().getHostAndPort());
//
//        topology.stopAndWait();

        byte[] row = "party".getBytes(Charsets.UTF_8);
        byte[] table = "MYTABLE".getBytes(Charsets.UTF_8);

        byte[] regionName = HRegionInfo.createRegionName(table, row, HConstants.NINES, false);

        System.out.println(new String(regionName, Charsets.UTF_8));
    }
}
