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
        byte[] tableBytes = table.getBytes(Charsets.UTF_8);

        // TODO: the last parameter in the real client is set by tries != 0.  Probably need to do retries on
        // TODO: trying to get the region location
        try {
            return connection.getRegionLocation(tableBytes, targetRow, false);
        }
        catch (IOException e) {
            throw new RuntimeException("Error locating region location for get operation", e);
        }
    }
}
