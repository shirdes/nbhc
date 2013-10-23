package com.urbanairship.hbase.shc.scan;

import org.apache.hadoop.hbase.HRegionLocation;

public interface ScanResultsLoader {

    ScannerBatchResult load(HRegionLocation location, long scannerId, int numResults);

}
