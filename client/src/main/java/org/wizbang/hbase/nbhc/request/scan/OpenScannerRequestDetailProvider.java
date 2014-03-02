package org.wizbang.hbase.nbhc.request.scan;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.wizbang.hbase.nbhc.Protocol;
import org.wizbang.hbase.nbhc.request.RequestDetailProvider;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

import java.util.concurrent.atomic.AtomicReference;

import static org.wizbang.hbase.nbhc.Protocol.OPEN_SCANNER_TARGET_METHOD;
import static org.wizbang.hbase.nbhc.Protocol.TARGET_PROTOCOL;

public final class OpenScannerRequestDetailProvider implements RequestDetailProvider {

    private final AtomicReference<HRegionLocation> currentLocation = new AtomicReference<HRegionLocation>(null);

    private final String table;
    private final Scan scan;
    private final RegionOwnershipTopology topology;

    OpenScannerRequestDetailProvider(String table, Scan scan, RegionOwnershipTopology topology) {
        this.table = table;
        this.scan = scan;
        this.topology = topology;
    }

    @Override
    public HRegionLocation getLocation() {
        return capture(topology.getRegionServer(table, scan.getStartRow()));
    }

    @Override
    public HRegionLocation getRetryLocation() {
        return capture(topology.getRegionServerNoCache(table, scan.getStartRow()));
    }

    private HRegionLocation capture(HRegionLocation location) {
        currentLocation.set(location);
        return location;
    }

    @Override
    public Invocation getInvocation(HRegionLocation targetLocation) {
        return new Invocation(OPEN_SCANNER_TARGET_METHOD, TARGET_PROTOCOL, new Object[] {
                targetLocation.getRegionInfo().getRegionName(),
                scan
        });
    }

    @Override
    public ImmutableSet<Class<? extends Exception>> getLocationErrors() {
        return Protocol.STANDARD_LOCATION_ERRORS;
    }

    public HRegionLocation getLastRequestLocation() {
        HRegionLocation location = currentLocation.get();
        Preconditions.checkState(location != null);
        return location;
    }
}
