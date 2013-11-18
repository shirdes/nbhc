package com.urbanairship.hbase.shc.topology;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Result;

public class RootTableLookupSource {

    private final Supplier<HRegionLocation> masterLocationSupplier = new Supplier<HRegionLocation>() {
        @Override
        public HRegionLocation get() {
            ServerName masterServer = clusterTopology.getMasterServer();
            return new HRegionLocation(HRegionInfo.ROOT_REGIONINFO, masterServer.getHostname(),
                    masterServer.getPort());
        }
    };

    private final HbaseClusterTopology clusterTopology;
    private final TopologyOperations operationsClient;
    private final TopologyUtil util;

    public RootTableLookupSource(HbaseClusterTopology clusterTopology,
                                 TopologyOperations operationsClient,
                                 TopologyUtil util) {
        this.clusterTopology = clusterTopology;
        this.operationsClient = operationsClient;
        this.util = util;
    }

    public HRegionLocation getMetaLocation(byte[] metaTableKey) {
        byte[] rootKey = HRegionInfo.createRegionName(HConstants.META_TABLE_NAME, metaTableKey, HConstants.NINES, false);

        Optional<Result> metaRegionInfoRow = operationsClient.getRowOrBefore(rootKey, masterLocationSupplier);
        if (!metaRegionInfoRow.isPresent()) {
            throw new RuntimeException("Failed to retrieve -ROOT- table row for meta data");
        }

        TopologyResult result = util.extractLocation(metaRegionInfoRow.get());

        return result.getLocation();
    }
}
