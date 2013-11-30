package org.wizbang.hbase.nbhc.request.scan;

import org.apache.hadoop.hbase.HRegionLocation;

public final class ScannerOpenResult {

    private final HRegionLocation location;
    private final long scannerId;

    public ScannerOpenResult(HRegionLocation location, long scannerId) {
        this.location = location;
        this.scannerId = scannerId;
    }

    public HRegionLocation getLocation() {
        return location;
    }

    public long getScannerId() {
        return scannerId;
    }
}
