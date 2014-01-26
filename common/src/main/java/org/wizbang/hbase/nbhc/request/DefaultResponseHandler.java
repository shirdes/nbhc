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
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.response.RemoteError;

public final class DefaultResponseHandler<R> implements ResponseHandler {

    private static final Logger log = LogManager.getLogger(DefaultResponseHandler.class);

    private final ResultBroker<R> resultBroker;
    private final Function<HRegionLocation, Invocation> invocationBuilder;
    private final ResponseProcessor<R> responseProcessor;
    private final Supplier<HRegionLocation> updatedLocationSupplier;
    private final RequestSender sender;
    private final RetryExecutor retryExecutor;
    private final HbaseClientConfiguration config;

    private HRegionLocation currentLocation;

    public DefaultResponseHandler(HRegionLocation location,
                                  ResultBroker<R> resultBroker,
                                  Function<HRegionLocation, Invocation> invocationBuilder,
                                  ResponseProcessor<R> responseProcessor,
                                  Supplier<HRegionLocation> updatedLocationSupplier,
                                  RequestSender sender,
                                  RetryExecutor retryExecutor,
                                  HbaseClientConfiguration config) {
        this.resultBroker = resultBroker;
        this.invocationBuilder = invocationBuilder;
        this.responseProcessor = responseProcessor;
        this.updatedLocationSupplier = updatedLocationSupplier;
        this.sender = sender;
        this.retryExecutor = retryExecutor;
        this.config = config;

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
            handleLocationError(error, attempt);
        }
        else {
            resultBroker.communicateError(constructRemoteException(error));
        }
    }

    private void handleLocationError(RemoteError error, int attempt) {
        if (attempt <= config.maxLocationErrorRetries) {
            retryOperation(attempt, error);
        }
        else {
            resultBroker.communicateError(new RuntimeException(
                String.format("Max attempts of %d for operation reached!", config.maxLocationErrorRetries),
                constructRemoteException(error))
            );
        }
    }

    private RemoteException constructRemoteException(RemoteError error) {
        return new RemoteException(error.getErrorClass(),
                error.getErrorMessage().isPresent() ? error.getErrorMessage().get() : "");
    }

    private void retryOperation(final int attempt, RemoteError locationError) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Attempt %d failed with a retriable region location error", attempt), constructRemoteException(locationError));
        }

        Runnable retry = new Runnable() {
            @Override
            public void run() {
                executeRetry(attempt);
            }
        };

        retryExecutor.retry(retry);
    }

    private void executeRetry(int attempt) {
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
