package com.urbanairship.hbase.shc.topology;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.urbanairship.hbase.shc.dispatch.HbaseOperationResultFuture;
import com.urbanairship.hbase.shc.dispatch.RequestManager;
import com.urbanairship.hbase.shc.request.DefaultRequestController;
import com.urbanairship.hbase.shc.request.RequestSender;
import com.urbanairship.hbase.shc.request.SimpleParseResponseProcessor;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import java.io.IOException;

import static com.urbanairship.hbase.shc.Protocol.*;

public class RootTable {

    private final HbaseClusterTopology clusterTopology;
    private final RequestSender sender;
    private final RequestManager requestManager;
    private final int maxRetries;

    public RootTable(HbaseClusterTopology clusterTopology,
                     RequestSender sender,
                     RequestManager requestManager,
                     int maxRetries) {
        this.clusterTopology = clusterTopology;
        this.sender = sender;
        this.requestManager = requestManager;
        this.maxRetries = maxRetries;
    }

    public HRegionLocation getMetaLocation(byte[] metaTableKey) {
        byte[] rootKey = HRegionInfo.createRegionName(HConstants.META_TABLE_NAME, metaTableKey, HConstants.NINES, false);

        Result metaRegionInfoRow = getMetaRegionInfoRow(rootKey);
        if (metaRegionInfoRow == null || metaRegionInfoRow.isEmpty()) {
            throw new RuntimeException("Failed to retrieve root -ROOT- table row for meta data");
        }

        HRegionInfo regionInfo = parseRegionInfo(metaRegionInfoRow);
        if (regionInfo.isSplit()) {
            throw new RuntimeException("the only available region for the required row is a split parent," +
                    " the daughters should be online soon: " + regionInfo.getRegionNameAsString());
        }
        if (regionInfo.isOffline()) {
            throw new RuntimeException("the region is offline, could be caused by a disable table call: " +
                    regionInfo.getRegionNameAsString());
        }

        // TODO: caching?
        return parseLocation(regionInfo, metaRegionInfoRow);
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
        byte [] value = metaRegionInfoRow.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER);
        if (value == null || value.length == 0) {
            throw new RuntimeException("HRegionInfo was null or empty in -ROOT- table, row=" + metaRegionInfoRow);
        }
        // convert the row result into the HRegionLocation we need!
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

    private Result getMetaRegionInfoRow(byte[] rootKey) {

        Supplier<HRegionLocation> masterLocationSupplier = new Supplier<HRegionLocation>() {
            @Override
            public HRegionLocation get() {
                ServerName masterServer = clusterTopology.getMasterServer();
                return new HRegionLocation(HRegionInfo.ROOT_REGIONINFO, masterServer.getHostname(),
                        masterServer.getPort());
            }
        };

        HRegionLocation location = masterLocationSupplier.get();
        final Invocation invocation = new Invocation(GET_CLOSEST_ROW_BEFORE_METHOD, TARGET_PROTOCOL, new Object[]{
                location.getRegionInfo().getRegionName(),
                rootKey,
                HConstants.CATALOG_FAMILY
        });

        Function<HRegionLocation, Invocation> invocationBuilder = new Function<HRegionLocation, Invocation>() {
            @Override
            public Invocation apply(HRegionLocation location) {
                return invocation;
            }
        };

        HbaseOperationResultFuture<Result> future = new HbaseOperationResultFuture<Result>(requestManager);
        SimpleParseResponseProcessor<Result> processor = new SimpleParseResponseProcessor<Result>(GET_CLOSEST_ROW_BEFORE_RESPONSE_PARSER);
        DefaultRequestController<Result> controller = new DefaultRequestController<Result>(
                location,
                future,
                invocationBuilder,
                processor,
                masterLocationSupplier,
                sender,
                maxRetries
        );

        sender.sendRequest(location, invocation, future, controller, 1);

        // TODO: need a timeout
        try {
            return future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for request to retrieve root table location for key");
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to retrieve root table location for key");
        }
    }

}
