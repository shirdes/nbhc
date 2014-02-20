package org.wizbang.hbase.nbhc.request.scan;

import org.apache.hadoop.hbase.client.Scan;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.request.SingleActionRequestInitiator;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

public class ScannerInitiator {

    private final RegionOwnershipTopology topology;
    private final SingleActionRequestInitiator singleActionRequestInitiator;
    private final HbaseClientConfiguration config;

    public ScannerInitiator(RegionOwnershipTopology topology,
                            SingleActionRequestInitiator singleActionRequestInitiator,
                            HbaseClientConfiguration config) {
        this.topology = topology;
        this.singleActionRequestInitiator = singleActionRequestInitiator;
        this.config = config;
    }

    public ScannerResultStream initiate(String table, Scan scan) {
        ScanCoordinator coordinator = new ScanCoordinator(
                table,
                scan,
                topology,
                singleActionRequestInitiator,
                config
        );

        return new ScannerResultStream(coordinator);
    }
}
