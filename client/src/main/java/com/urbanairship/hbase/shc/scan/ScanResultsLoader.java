package com.urbanairship.hbase.shc.scan;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HRegionLocation;

public interface ScanResultsLoader {

    ListenableFuture<ScannerBatchResult> load(HRegionLocation location, long scannerId);

}
