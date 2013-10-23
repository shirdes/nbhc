package com.urbanairship.hbase.shc.scan;

import com.urbanairship.hbase.shc.RegionOwnershipTopology;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;

public final class ScanStateHolder {

    private final String table;
    private final RegionOwnershipTopology topology;
    private final ScanOpener opener;
    private final ScanResultsLoader resultsLoader;
    private final int batchSize;

    private HRegionLocation currentLocation = null;
    private Scan currentScan;
    private long currentScannerId;

    public void openNextScannerId() {
        if (currentLocation == null) {
            currentLocation = topology.getRegionServer(table, currentScan.getStartRow());
        }
        else {
            // TODO: get the last row of the current region and move the scan just beyond it and get the location
        }

        currentScannerId = opener.openScanner(table, currentScan);
    }

    public ScannerBatchResult loadNextBatch() {
        return resultsLoader.load(currentLocation, currentScannerId, batchSize);
    }

}
