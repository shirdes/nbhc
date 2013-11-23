package org.wizbang.hbase.nbhc.topology;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.HRegionLocation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MetaTable implements RegionOwnershipTopology {

    private final ConcurrentMap<String, LocationCache> tableLocationCaches = new ConcurrentHashMap<String, LocationCache>();

    private final Function<byte[], HRegionLocation> defaultRootTableLookup = new Function<byte[], HRegionLocation>() {
        @Override
        public HRegionLocation apply(byte[] metaKey) {
            return rootTable.getMetaLocation(metaKey);
        }
    };

    private final Function<byte[], HRegionLocation> rootTableUncachedLookup = new Function<byte[], HRegionLocation>() {
        @Override
        public HRegionLocation apply(byte[] metaKey) {
            return rootTable.getMetaLocationNoCache(metaKey);
        }
    };

    private final MetaTableLookupSource metaSource;
    private final Supplier<LocationCache> cacheBuilder;
    private final RootTable rootTable;

    public MetaTable(MetaTableLookupSource metaSource, Supplier<LocationCache> cacheBuilder, RootTable rootTable) {
        this.metaSource = metaSource;
        this.cacheBuilder = cacheBuilder;
        this.rootTable = rootTable;
    }

    @Override
    public HRegionLocation getRegionServer(String table, byte[] targetRow) {
        LocationCache cache = getCache(table);
        Optional<HRegionLocation> cached = cache.getCachedLocation(targetRow);
        if (cached.isPresent()) {
            return cached.get();
        }

        return lookup(table, targetRow, cache, defaultRootTableLookup);
    }

    @Override
    public HRegionLocation getRegionServerNoCache(String table, byte[] targetRow) {
        LocationCache cache = getCache(table);
        cache.removeLocationContainingRow(targetRow);

        return lookup(table, targetRow, cache, rootTableUncachedLookup);
    }

    private LocationCache getCache(String table) {
        LocationCache cache = tableLocationCaches.get(table);
        if (cache != null) {
            return cache;
        }

        cache = cacheBuilder.get();
        LocationCache had = tableLocationCaches.putIfAbsent(table, cache);
        return had == null ? cache : had;
    }

    private HRegionLocation lookup(String table,
                                   byte[] row,
                                   LocationCache cache,
                                   Function<byte[], HRegionLocation> rootTableLookup) {
        HRegionLocation location = metaSource.getLocation(table, row, rootTableLookup);
        cache.cacheLocation(location);

        return location;
    }
}
