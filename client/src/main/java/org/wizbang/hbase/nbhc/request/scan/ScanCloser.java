package org.wizbang.hbase.nbhc.request.scan;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HRegionLocation;

public interface ScanCloser {

    ListenableFuture<Void> close(HRegionLocation location, long scannerId);

}