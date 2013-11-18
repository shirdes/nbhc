package com.urbanairship.hbase.shc;

import com.codahale.metrics.*;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public final class HbaseClientMetrics {

    private static final MetricRegistry REGISTRY = new MetricRegistry();
    static {
        JmxReporter.forRegistry(REGISTRY)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .build()
                .start();
    }

    private static final String PREFIX = "HBaseClientMetrics:";

    public static Timer timer(String name) {
        return REGISTRY.timer(prefixed(name));
    }

    public static Meter meter(String name) {
        return REGISTRY.meter(prefixed(name));
    }

    public static Histogram histogram(String name) {
        return REGISTRY.histogram(prefixed(name));
    }

    public static void gauge(String name, Gauge<?> gauge) {
        final String prefixed = prefixed(name);
        SortedMap<String,Gauge> existing = REGISTRY.getGauges(new MetricFilter() {
            @Override
            public boolean matches(String metricName, Metric metric) {
                return prefixed.equals(metricName);
            }
        });

        if (existing.isEmpty()) {
            REGISTRY.register(prefixed, gauge);
        }
    }

    private static String prefixed(String name) {
        return PREFIX + name;
    }

    private HbaseClientMetrics() { }
}
