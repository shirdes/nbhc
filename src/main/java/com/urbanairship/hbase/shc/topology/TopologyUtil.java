package com.urbanairship.hbase.shc.topology;

import com.google.common.net.HostAndPort;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import java.io.IOException;

public class TopologyUtil {

    public TopologyResult extractLocation(Result metaResult) {
        HRegionInfo regionInfo = parseRegionInfo(metaResult);
        if (regionInfo.isSplit()) {
            throw new RuntimeException("the only available region for the required row is a split parent," +
                    " the daughters should be online soon: " + regionInfo.getRegionNameAsString());
        }
        if (regionInfo.isOffline()) {
            throw new RuntimeException("the region is offline, could be caused by a disable table call: " +
                    regionInfo.getRegionNameAsString());
        }

        HRegionLocation location = parseLocation(regionInfo, metaResult);
        return new TopologyResult(regionInfo.getTableName(), location);
    }

    private HRegionLocation parseLocation(HRegionInfo regionInfo, Result metaRegionInfoRow) {
        byte[] value = metaRegionInfoRow.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);

        String rawHostAndPort = (value != null) ? Bytes.toString(value) : "";
        if (StringUtils.isBlank(rawHostAndPort)) {
            throw new RuntimeException("No server address listed in -ROOT- for region " +
                    regionInfo.getRegionNameAsString() + " containing row " +
                    Bytes.toStringBinary(metaRegionInfoRow.getRow()));
        }

        HostAndPort hostAndPort = HostAndPort.fromString(rawHostAndPort);
        return new HRegionLocation(regionInfo, hostAndPort.getHostText(), hostAndPort.getPort());

    }

    private HRegionInfo parseRegionInfo(Result metaRegionInfoRow) {
        byte[] value = metaRegionInfoRow.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
        if (value == null || value.length == 0) {
            throw new RuntimeException("HRegionInfo was null or empty in -ROOT- table, row=" + metaRegionInfoRow);
        }

        HRegionInfo regionInfo;
        try {
            regionInfo = (HRegionInfo) Writables.getWritable(value, new HRegionInfo());
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to parse HRegionInfo in -ROOT- table, row=" + metaRegionInfoRow);
        }

        // possible we got a region of a different table...
        // TODO: really??  That's would be really scary since it should be the META table
//        if (!Bytes.equals(regionInfo.getTableName(), HConstants.META_TABLE_NAME)) {
//            throw new TableNotFoundException(
//                    "Table '" + Bytes.toString(tableName) + "' was not found, got: " +
//                            Bytes.toString(regionInfo.getTableName()) + ".");
//        }

        return regionInfo;
    }
}
