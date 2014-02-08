package org.wizbang.hbase.nbhc.request;

import com.google.common.base.Function;
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
import org.wizbang.hbase.nbhc.response.RequestResponseController;

// TODO: maybe we should use a builder since we have so many parameters...
public final class SingleActionController<R> implements RequestResponseController {

    private static final Logger log = LogManager.getLogger(SingleActionController.class);

    private final RequestDetailProvider requestDetailProvider;
    private final ResultBroker<R> resultBroker;
    private final Function<HbaseObjectWritable, R> responseParser;
    private final RequestSender sender;
    private final RetryExecutor retryExecutor;
    private final HbaseClientConfiguration config;

    private int attempt = 0;

    public static <R> void initiate(RequestDetailProvider requestDetailProvider,
                                    ResultBroker<R> resultBroker,
                                    Function<HbaseObjectWritable, R> responseParser,
                                    RequestSender sender,
                                    RetryExecutor retryExecutor,
                                    HbaseClientConfiguration config) {

        SingleActionController<R> instance = new SingleActionController<R>(
                requestDetailProvider,
                resultBroker,
                responseParser,
                sender,
                retryExecutor,
                config
        );

        instance.launch();
    }

    private SingleActionController(RequestDetailProvider requestDetailProvider,
                                   ResultBroker<R> resultBroker,
                                   Function<HbaseObjectWritable, R> responseParser,
                                   RequestSender sender,
                                   RetryExecutor retryExecutor,
                                   HbaseClientConfiguration config) {
        this.requestDetailProvider = requestDetailProvider;
        this.resultBroker = resultBroker;
        this.responseParser = responseParser;
        this.sender = sender;
        this.retryExecutor = retryExecutor;
        this.config = config;
    }

    public void launch() {
        requestToLocation(requestDetailProvider.getLocation());
    }

    @Override
    public void receiveResponse(HbaseObjectWritable value) {
        try {
            R result = responseParser.apply(value);
            resultBroker.communicateResult(result);
        }
        catch (Exception e) {
            resultBroker.communicateError(e);
        }
    }

    @Override
    public void receiveRemoteError(RemoteError remoteError) {
        boolean locationError = isRegionLocationError(remoteError);
        if (locationError) {
            handleLocationError(remoteError, attempt);
        }
        else {
            resultBroker.communicateError(constructRemoteException(remoteError));
        }
    }

    @Override
    public void receiveLocalError(Throwable error) {
        resultBroker.communicateError(error);
    }

    private void handleLocationError(RemoteError error, int attempt) {
        if (attempt <= config.maxLocationErrorRetries) {
            retryOperation(error);
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

    private void retryOperation(RemoteError locationError) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Attempt %d failed with a retriable region location error", attempt), constructRemoteException(locationError));
        }

        Runnable retry = new Runnable() {
            @Override
            public void run() {
                executeRetry();
            }
        };

        retryExecutor.retry(retry);
    }

    private void executeRetry() {
        attempt++;
        requestToLocation(requestDetailProvider.getRetryLocation());
    }

    private void requestToLocation(HRegionLocation location) {
        Invocation invocation = requestDetailProvider.getInvocation(location);
        sender.sendRequest(location, invocation, this);
    }

    private boolean isRegionLocationError(RemoteError error) {
        return NotServingRegionException.class.getName().equals(error.getErrorClass()) ||
                RegionServerStoppedException.class.getName().equals(error.getErrorClass());
    }
}
