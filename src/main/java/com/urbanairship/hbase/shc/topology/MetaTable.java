package com.urbanairship.hbase.shc.topology;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Result;

import java.util.Arrays;

public class MetaTable {

    private final RootTable rootTable;
    private final TopologyOperationsClient operationsClient;
    private final TopologyUtil util;

    public MetaTable(RootTable rootTable,
                     TopologyOperationsClient operationsClient,
                     TopologyUtil util) {
        this.rootTable = rootTable;
        this.operationsClient = operationsClient;
        this.util = util;
    }

    public HRegionLocation getLocation(String table, byte[] row) {
        // TODO: cache lookup...

        byte[] tableNameBytes = table.getBytes(Charsets.UTF_8);
        final byte[] metaKey = HRegionInfo.createRegionName(tableNameBytes, row, HConstants.NINES, false);

        Supplier<HRegionLocation> locationSupplier = new Supplier<HRegionLocation>() {
            @Override
            public HRegionLocation get() {
                return rootTable.getMetaLocation(metaKey);
            }
        };

        // TODO: the code in the standard client does some prepopulation of meta data for the table.  However, for
        // TODO: some reason, it starts a scan of meta rows for the table only at the row in question and then goes
        // TODO: forward.  Seems that if we are going to prepopulate, we'd just go ahead and get all the meta rows
        // TODO: for the table each time??

        Optional<Result> metaRow = operationsClient.getRowOrBefore(metaKey, locationSupplier);
        if (!metaRow.isPresent()) {
            throw new RuntimeException(String.format("Table %s not found", table));
        }

        TopologyResult result = util.extractLocation(metaRow.get());
        if (!Arrays.equals(result.getTable(), tableNameBytes)) {
            throw new RuntimeException(String.format("Table '%s' was not found, got %s", table, new String(result.getTable(), Charsets.UTF_8)));
        }

        // TODO: caching?  Although if we prepopulate above, probably wouldn't need to here.

        return result.getLocation();
    }

}
