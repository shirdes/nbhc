package org.wizbang.hbase.nbhc.topology;

import com.codahale.metrics.Timer;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.HbaseClientMetrics;
import org.wizbang.hbase.nbhc.Protocol;
import org.wizbang.hbase.nbhc.request.RequestDetailProvider;
import org.wizbang.hbase.nbhc.request.SingleActionRequestInitiator;

import java.util.concurrent.TimeUnit;

import static org.wizbang.hbase.nbhc.Protocol.*;

public final class TopologyOperationsClient implements TopologyOperations {

    private static final Timer GET_ROW_OR_BEFORE_TIMER = HbaseClientMetrics.timer("TopologyOperations:GetRowOrBefore");

    private final SingleActionRequestInitiator singleActionRequestInitiator;
    private final HbaseClientConfiguration config;

    public TopologyOperationsClient(SingleActionRequestInitiator singleActionRequestInitiator,
                                    HbaseClientConfiguration config) {
        this.singleActionRequestInitiator = singleActionRequestInitiator;
        this.config = config;
    }

    @Override
    public Optional<Result> getRowOrBefore(final byte[] row, Supplier<HRegionLocation> locationSupplier) {
        final HRegionLocation location = locationSupplier.get();
        RequestDetailProvider requestDetailProvider = new RequestDetailProvider() {
            @Override
            public HRegionLocation getLocation() {
                return location;
            }

            @Override
            public HRegionLocation getRetryLocation() {
                return location;
            }

            @Override
            public Invocation getInvocation(HRegionLocation targetLocation) {
                return new Invocation(GET_CLOSEST_ROW_BEFORE_METHOD, TARGET_PROTOCOL, new Object[]{
                        targetLocation.getRegionInfo().getRegionName(),
                        row,
                        HConstants.CATALOG_FAMILY
                });
            }

            @Override
            public ImmutableSet<Class<? extends Exception>> getLocationErrors() {
                return Protocol.STANDARD_LOCATION_ERRORS;
            }
        };

        ListenableFuture<Result> future = singleActionRequestInitiator.initiate(requestDetailProvider,
                GET_CLOSEST_ROW_BEFORE_RESPONSE_PARSER);

        long start = System.currentTimeMillis();
        Result result;
        try {
            result = future.get(config.topologyOperationsTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for request to retrieve closest row or before for key " + new String(row, Charsets.UTF_8));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to retrieve closest row or before for key " + new String(row, Charsets.UTF_8), e);
        }
        finally {
            GET_ROW_OR_BEFORE_TIMER.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        }

        return (result == null || result.isEmpty())
                ? Optional.<Result>absent()
                : Optional.of(result);
    }
}
