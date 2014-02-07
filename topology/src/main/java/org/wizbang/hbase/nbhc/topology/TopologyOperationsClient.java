package org.wizbang.hbase.nbhc.topology;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.HbaseOperationResultFuture;
import org.wizbang.hbase.nbhc.request.OperationFutureSupplier;
import org.wizbang.hbase.nbhc.request.RequestDetailProvider;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.request.SingleActionController;

import static org.wizbang.hbase.nbhc.Protocol.*;

public class TopologyOperationsClient implements TopologyOperations {

    private final RequestSender sender;
    private final OperationFutureSupplier futureSupplier;
    private final RetryExecutor retryExecutor;
    private final HbaseClientConfiguration config;

    public TopologyOperationsClient(RequestSender sender,
                                    OperationFutureSupplier futureSupplier,
                                    RetryExecutor retryExecutor,
                                    HbaseClientConfiguration config) {
        this.sender = sender;
        this.futureSupplier = futureSupplier;
        this.retryExecutor = retryExecutor;
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
        };

        HbaseOperationResultFuture<Result> future = futureSupplier.create();
        SingleActionController.initiate(
                requestDetailProvider,
                future,
                GET_CLOSEST_ROW_BEFORE_RESPONSE_PARSER,
                sender,
                retryExecutor,
                config
        );

        // TODO: need a timeout
        Result result;
        try {
            result = future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for request to retrieve closest row or before for key " + new String(row, Charsets.UTF_8));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to retrieve closest row or before for key " + new String(row, Charsets.UTF_8), e);
        }

        return (result == null || result.isEmpty())
                ? Optional.<Result>absent()
                : Optional.of(result);
    }
}
