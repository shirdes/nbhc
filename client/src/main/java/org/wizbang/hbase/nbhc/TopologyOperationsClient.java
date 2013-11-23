package org.wizbang.hbase.nbhc;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.wizbang.hbase.nbhc.dispatch.HbaseOperationResultFuture;
import org.wizbang.hbase.nbhc.request.DefaultRequestController;
import org.wizbang.hbase.nbhc.request.OperationFutureSupplier;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.request.SimpleParseResponseProcessor;
import org.wizbang.hbase.nbhc.topology.TopologyOperations;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.Invocation;

import static org.wizbang.hbase.nbhc.Protocol.*;

public class TopologyOperationsClient implements TopologyOperations {

    private final RequestSender sender;
    private final OperationFutureSupplier futureSupplier;
    private final int maxRetries;

    public TopologyOperationsClient(RequestSender sender, OperationFutureSupplier futureSupplier, int maxRetries) {
        this.sender = sender;
        this.futureSupplier = futureSupplier;
        this.maxRetries = maxRetries;
    }

    @Override
    public Optional<Result> getRowOrBefore(byte[] row, Supplier<HRegionLocation> locationSupplier) {
        HRegionLocation location = locationSupplier.get();
        final Invocation invocation = new Invocation(GET_CLOSEST_ROW_BEFORE_METHOD, TARGET_PROTOCOL, new Object[]{
                location.getRegionInfo().getRegionName(),
                row,
                HConstants.CATALOG_FAMILY
        });

        Function<HRegionLocation, Invocation> invocationBuilder = new Function<HRegionLocation, Invocation>() {
            @Override
            public Invocation apply(HRegionLocation location) {
                return invocation;
            }
        };

        HbaseOperationResultFuture<Result> future = futureSupplier.create();
        SimpleParseResponseProcessor<Result> processor = new SimpleParseResponseProcessor<Result>(GET_CLOSEST_ROW_BEFORE_RESPONSE_PARSER);
        DefaultRequestController<Result> controller = new DefaultRequestController<Result>(
                location,
                future,
                invocationBuilder,
                processor,
                locationSupplier,
                sender,
                maxRetries
        );

        sender.sendRequest(location, invocation, future, controller, 1);

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
