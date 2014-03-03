package org.wizbang.hbase.nbhc.topology;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import org.apache.hadoop.hbase.HRegionLocation;
import org.wizbang.hbase.nbhc.HbaseClientMetrics;

import java.util.concurrent.TimeUnit;

public class RootTable {

    private static final Meter CACHE_LOOKUPS_METER = HbaseClientMetrics.meter("RootTable:CacheLookups");
    private static final Meter CACHE_HITS_METER = HbaseClientMetrics.meter("RootTable:CacheHits");
    private static final Meter BYPASS_CACHE_LOOKUPS_METER = HbaseClientMetrics.meter("RootTable:BypassCacheLookups");
    private static final Timer RETRIEVE_FROM_SOURCE_TIMER = HbaseClientMetrics.timer("RootTable:RetrieveMetaLocationFromSource");
    static {
        HbaseClientMetrics.gauge("RootTable:CacheHitPercent", new Gauge<Double>() {
            @Override
            public Double getValue() {
                long total = CACHE_LOOKUPS_METER.getCount();
                long hits = CACHE_LOOKUPS_METER.getCount();
                return (double) hits / (double) total;
            }
        });
    }

    private final RootTableLookupSource source;
    private final LocationCache cache;

    public RootTable(RootTableLookupSource source, LocationCache cache) {
        this.source = source;
        this.cache = cache;
    }

    public HRegionLocation getMetaLocation(byte[] metaKey) {
        CACHE_LOOKUPS_METER.mark();
        Optional<HRegionLocation> cached = cache.getCachedLocation(metaKey);
        if (cached.isPresent()) {
            CACHE_HITS_METER.mark();
            return cached.get();
        }

        return getFromSource(metaKey);
    }

    public HRegionLocation getMetaLocationNoCache(byte[] metaKey) {
        BYPASS_CACHE_LOOKUPS_METER.mark();
        cache.removeLocationContainingRow(metaKey);

        return getFromSource(metaKey);
    }

    private HRegionLocation getFromSource(byte[] metaKey) {
        long start = System.currentTimeMillis();
        HRegionLocation location = source.getMetaLocation(metaKey);
        RETRIEVE_FROM_SOURCE_TIMER.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);

        cache.cacheLocation(location);

        return location;
    }
}
