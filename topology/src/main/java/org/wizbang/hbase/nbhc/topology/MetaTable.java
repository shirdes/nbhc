package org.wizbang.hbase.nbhc.topology;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.HRegionLocation;
import org.wizbang.hbase.nbhc.HbaseClientMetrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class MetaTable implements RegionOwnershipTopology {

    private static final Meter CACHE_LOOKUPS_METER = HbaseClientMetrics.meter("MetaTable:CacheLookups");
    private static final Meter CACHE_HITS_METER = HbaseClientMetrics.meter("MetaTable:CacheHits");
    private static final Timer RETRIEVE_FROM_SOURCE_TIMER = HbaseClientMetrics.timer("MetaTable:RetrieveFromSource");
    private static final Meter BYPASS_CACHE_LOOKUPS_METER = HbaseClientMetrics.meter("MetaTable:BypassCacheLookups");
    static {
        HbaseClientMetrics.gauge("MetaTable:CacheHitPercent", new Gauge<Double>() {
            @Override
            public Double getValue() {
                long total = CACHE_LOOKUPS_METER.getCount();
                long hits = CACHE_HITS_METER.getCount();
                return (double) hits / (double) total;
            }
        });
    }

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
        CACHE_LOOKUPS_METER.mark();
        LocationCache cache = getCache(table);
        Optional<HRegionLocation> cached = cache.getCachedLocation(targetRow);
        if (cached.isPresent()) {
            CACHE_HITS_METER.mark();
            return cached.get();
        }

        return lookup(table, targetRow, cache, defaultRootTableLookup);
    }

    @Override
    public HRegionLocation getRegionServerNoCache(String table, byte[] targetRow) {
        BYPASS_CACHE_LOOKUPS_METER.mark();
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
        long start = System.currentTimeMillis();
        HRegionLocation location = metaSource.getLocation(table, row, rootTableLookup);
        RETRIEVE_FROM_SOURCE_TIMER.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);

        cache.cacheLocation(location);

        return location;
    }
}
