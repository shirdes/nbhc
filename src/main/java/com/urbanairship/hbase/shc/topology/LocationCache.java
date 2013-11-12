package com.urbanairship.hbase.shc.topology;

import com.google.common.base.Optional;
import com.google.common.primitives.UnsignedBytes;
import org.apache.hadoop.hbase.HRegionLocation;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

// TODO: the cache in the standard client uses SoftReferences.  Seems like might be overkill?
public class LocationCache {

    private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();

    private final ConcurrentNavigableMap<byte[], HRegionLocation> cache;

    public LocationCache() {
        this.cache = new ConcurrentSkipListMap<byte[], HRegionLocation>(COMPARATOR);
    }

    public Optional<HRegionLocation> getCachedLocation(byte[] row) {
        Map.Entry<byte[], HRegionLocation> entry = cache.floorEntry(row);
        if (entry == null) {
            return Optional.absent();
        }

        // We know the row is after the start key of the region so if the region is the end, the key is in this region
        HRegionLocation location = entry.getValue();
        byte[] locationEndKey = location.getRegionInfo().getEndKey();
        if (isLastRegionOfTable(locationEndKey)) {
            return Optional.of(location);
        }

        // Ensure the key is less than the end key of the region
        int compare = COMPARATOR.compare(row, locationEndKey);
        return (compare < 0) ? Optional.of(location) : Optional.<HRegionLocation>absent();
    }

    private boolean isLastRegionOfTable(byte[] endKey) {
        return endKey.length == 0;  // TODO: is this ok vs. doing an actual comparison against empty byte array?
    }

    public void cacheLocation(HRegionLocation location) {
        cache.put(location.getRegionInfo().getStartKey(), location);
    }

    public void removeLocationContainingRow(byte[] row) {
        Optional<HRegionLocation> location = getCachedLocation(row);
        if (location.isPresent()) {
            cache.remove(location.get().getRegionInfo().getStartKey());
        }
    }
}
