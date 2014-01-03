package org.wizbang.hbase.nbhc.request;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.response.RemoteError;

public final class DefaultResponseHandler<R> implements ResponseHandler {

    private static final Logger log = LogManager.getLogger(DefaultResponseHandler.class);

    private final ResultBroker<R> resultBroker;
    private final Function<HRegionLocation, Invocation> invocationBuilder;
    private final ResponseProcessor<R> responseProcessor;
    private final Supplier<HRegionLocation> updatedLocationSupplier;
    private final RequestSender sender;
    private final int maxRetries;

    private HRegionLocation currentLocation;

    public DefaultResponseHandler(HRegionLocation location,
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
        boolean locationError = isRegionLocationError(error);
        if (locationError) {
            if (attempt <= maxRetries) {
                retryOperation(attempt, error);
            }
            else {
                resultBroker.communicateError(new RuntimeException(
                    String.format("Max attempts of %d for operation reached!", maxRetries),
                    constructRemoteException(error))
                );
            }
        }
        else {
            resultBroker.communicateError(constructRemoteException(error));
        }
    }

    private RemoteException constructRemoteException(RemoteError error) {
        return new RemoteException(error.getErrorClass(),
                error.getErrorMessage().isPresent() ? error.getErrorMessage().get() : "");
    }

    private void retryOperation(int attempt, RemoteError locationError) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Attempt %d failed with a retriable region location error", attempt), constructRemoteException(locationError));
        }

        currentLocation = updatedLocationSupplier.get();
        Invocation invocation = invocationBuilder.apply(currentLocation);
        sender.sendRequestForBroker(currentLocation, invocation, resultBroker, this, attempt + 1);
    }

    private boolean isRegionLocationError(RemoteError error) {
        return NotServingRegionException.class.getName().equals(error.getErrorClass()) ||
                RegionServerStoppedException.class.getName().equals(error.getErrorClass());
    }

    @Override
    public void handleLocalError(Throwable error, int attempt) {
        resultBroker.communicateError(error);
    }
}
