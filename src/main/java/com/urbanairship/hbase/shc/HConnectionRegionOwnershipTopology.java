package com.urbanairship.hbase.shc;

import com.google.common.base.Charsets;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HConnection;

import java.io.IOException;

public final class HConnectionRegionOwnershipTopology implements RegionOwnershipTopology {

    private final HConnection connection;

    public HConnectionRegionOwnershipTopology(HConnection connection) {
        this.connection = connection;
    }

    @Override
    public HRegionLocation getRegionServer(String table, byte[] targetRow) {
        return gethRegionLocationImpl(table, targetRow, false);
    }

    @Override
    public HRegionLocation getRegionServerNoCache(String table, byte[] targetRow) {
        return gethRegionLocationImpl(table, targetRow, true);
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
