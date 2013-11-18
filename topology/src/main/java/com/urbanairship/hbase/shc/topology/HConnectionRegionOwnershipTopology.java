package com.urbanairship.hbase.shc.topology;

import com.codahale.metrics.Timer;
import com.google.common.base.Charsets;
import com.urbanairship.hbase.shc.HbaseClientMetrics;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HConnection;

import java.io.IOException;

public final class HConnectionRegionOwnershipTopology implements RegionOwnershipTopology {

    private static final Timer GET_LOCATION_CACHED = HbaseClientMetrics.timer("HConnection:GetLocationCached");
    private static final Timer GET_LOCATION_UNCACHED = HbaseClientMetrics.timer("HConnection:GetLocationUncached");

    private final HConnection connection;

    public HConnectionRegionOwnershipTopology(HConnection connection) {
        this.connection = connection;
    }

    @Override
    public HRegionLocation getRegionServer(String table, byte[] targetRow) {
        Timer.Context time = GET_LOCATION_CACHED.time();
        try {
            return gethRegionLocationImpl(table, targetRow, false);
        }
        finally {
            time.stop();
        }
    }

    @Override
    public HRegionLocation getRegionServerNoCache(String table, byte[] targetRow) {
        Timer.Context time = GET_LOCATION_UNCACHED.time();
        try {
            return gethRegionLocationImpl(table, targetRow, true);
        }
        finally {
            time.stop();
        }
    }

    private HRegionLocation gethRegionLocationImpl(String table, byte[] targetRow, boolean skipCache) {
        byte[] tableBytes = table.getBytes(Charsets.UTF_8);

        try {
            return connection.getRegionLocation(tableBytes, targetRow, skipCache);
        }
        catch (IOException e) {
            throw new RuntimeException("Error locating region location for get operation", e);
        }
    }
}
