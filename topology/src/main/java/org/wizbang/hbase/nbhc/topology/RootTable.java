package org.wizbang.hbase.nbhc.topology;

import com.google.common.base.Optional;
import org.apache.hadoop.hbase.HRegionLocation;

public class RootTable {

    private final RootTableLookupSource source;
    private final LocationCache cache;

    public RootTable(RootTableLookupSource source, LocationCache cache) {
        this.source = source;
        this.cache = cache;
    }

    public HRegionLocation getMetaLocation(byte[] metaKey) {
        Optional<HRegionLocation> cached = cache.getCachedLocation(metaKey);
        if (cached.isPresent()) {
            return cached.get();
        }

        return getFromSource(metaKey);
    }

    public HRegionLocation getMetaLocationNoCache(byte[] metaKey) {
        cache.removeLocationContainingRow(metaKey);

        return getFromSource(metaKey);
    }

    private HRegionLocation getFromSource(byte[] metaKey) {
        HRegionLocation location = source.getMetaLocation(metaKey);

        cache.cacheLocation(location);

        return location;
    }
}
