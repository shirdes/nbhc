package com.urbanairship.hbase.shc.request;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.urbanairship.hbase.shc.dispatch.ResultBroker;
import com.urbanairship.hbase.shc.response.RemoteError;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.ipc.RemoteException;

public class DefaultRequestController<R> implements RequestController {

    private final ResultBroker<R> resultBroker;
    private final Function<HRegionLocation, Invocation> invocationBuilder;
    private final ResponseProcessor<R> responseProcessor;
    private final Supplier<HRegionLocation> updatedLocationSupplier;
    private final RequestSender sender;
    private final int maxRetries;

    private HRegionLocation currentLocation;

    public DefaultRequestController(HRegionLocation location,
                                     ResultBroker<R> resultBroker,
                                     Function<HRegionLocation, Invocation> invocationBuilder,
                                     ResponseProcessor<R> responseProcessor,
                                     Supplier<HRegionLocation> updatedLocationSupplier,
                                     RequestSender sender,
                                     int maxRetries) {
        this.resultBroker = resultBroker;
        this.invocationBuilder = invocationBuilder;
        this.responseProcessor = responseProcessor;
        this.updatedLocationSupplier = updatedLocationSupplier;
        this.sender = sender;
        this.maxRetries = maxRetries;

        this.currentLocation = location;
    }

    @Override
    public void handleResponse(HbaseObjectWritable received) {
        responseProcessor.process(currentLocation, received, resultBroker);
    }

    @Override
    public void handleRemoteError(RemoteError error, int attempt) {
        if (isRetryableError(error) && attempt <= maxRetries) {
            currentLocation = updatedLocationSupplier.get();
            Invocation invocation = invocationBuilder.apply(currentLocation);
            sender.sendRequest(currentLocation, invocation, resultBroker, this, attempt + 1);
        }
        else {
            // TODO; get smarter about the error handling
            resultBroker.communicateError(new RemoteException(error.getErrorClass(),
                    error.getErrorMessage().isPresent() ? error.getErrorMessage().get() : ""));
        }
    }

    private boolean isRetryableError(RemoteError error) {
        return false;
    }

    @Override
    public void handleLocalError(Throwable error, int attempt) {
        // TODO: implement.  Probably retry same as the remote error
    }
}
