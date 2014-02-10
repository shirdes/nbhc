package org.wizbang.hbase.nbhc.request.scan;

import org.apache.hadoop.hbase.client.Scan;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.request.SingleActionRequestInitiator;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

public class ScannerInitiator {

    private final RequestSender sender;
    private final RequestManager requestManager;
    private final RetryExecutor retryExecutor;
    private final RegionOwnershipTopology topology;
    private final SingleActionRequestInitiator singleActionRequestInitiator;
    private final HbaseClientConfiguration config;

    public ScannerInitiator(RequestSender sender,
                            RequestManager requestManager,
                            RetryExecutor retryExecutor,
                            RegionOwnershipTopology topology,
                            SingleActionRequestInitiator singleActionRequestInitiator,
                            HbaseClientConfiguration config) {
        this.sender = sender;
        this.requestManager = requestManager;
        this.retryExecutor = retryExecutor;
        this.topology = topology;
        this.singleActionRequestInitiator = singleActionRequestInitiator;
        this.config = config;
    }

    public ScannerResultStream initiate(String table, Scan scan) {
        ScanCoordinator coordinator = new ScanCoordinator(
                table,
                scan,
                sender,
                requestManager,
                retryExecutor,
                topology,
                singleActionRequestInitiator,
                config
        );

        return new ScannerResultStream(coordinator);
    }
}
