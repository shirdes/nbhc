package org.wizbang.hbase.nbhc.request;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.response.RequestResponseController;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class SingleActionController<R> implements RequestResponseController {

    private static final Logger log = LogManager.getLogger(SingleActionController.class);

    private final RequestDetailProvider requestDetailProvider;
    private final ResultBroker<R> resultBroker;
    private final Function<HbaseObjectWritable, R> responseParser;
    private final RequestSender sender;
    private final RetryExecutor retryExecutor;
    private final RequestManager requestManager;
    private final HbaseClientConfiguration config;

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final AtomicInteger activeRequestId = new AtomicInteger();

    private int attempt = 0;

    public static <R> SingleActionController<R> initiate(RequestDetailProvider requestDetailProvider,
                                                         ResultBroker<R> resultBroker,
                                                         Function<HbaseObjectWritable, R> responseParser,
                                                         RequestSender sender,
                                                         RetryExecutor retryExecutor,
                                                         RequestManager requestManager,
                                                         HbaseClientConfiguration config) {

        SingleActionController<R> instance = new SingleActionController<R>(
                requestDetailProvider,
                resultBroker,
                responseParser,
                sender,
                retryExecutor,
                requestManager,
                config
        );

        instance.launch();

        return instance;
    }

    private SingleActionController(RequestDetailProvider requestDetailProvider,
                                   ResultBroker<R> resultBroker,
                                   Function<HbaseObjectWritable, R> responseParser,
                                   RequestSender sender,
                                   RetryExecutor retryExecutor,
                                   RequestManager requestManager,
                                   HbaseClientConfiguration config) {
        this.requestDetailProvider = requestDetailProvider;
        this.resultBroker = resultBroker;
        this.responseParser = responseParser;
        this.sender = sender;
        this.retryExecutor = retryExecutor;
        this.requestManager = requestManager;
        this.config = config;
    }

    public void launch() {
        requestToLocation(requestDetailProvider.getLocation());
    }

    @Override
    public void receiveResponse(int requestId, HbaseObjectWritable value) {
        try {
            R result = responseParser.apply(value);
            resultBroker.communicateResult(result);
        }
        catch (Exception e) {
            resultBroker.communicateError(e);
        }
    }

    @Override
    public void receiveRemoteError(int requestId, RemoteError remoteError) {
        if (isRetryError(remoteError)) {
            handleRetry(remoteError, attempt);
        }
        else {
            resultBroker.communicateError(constructRemoteException(remoteError));
        }
    }

    @Override
    public void receiveCommunicationError(int requestId, Throwable error) {
        // TODO: implement, this should be retriable typically?
    }

    @Override
    public void receiveFatalError(int requestId, Throwable error) {
        resultBroker.communicateError(error);
    }

    @Override
    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            requestManager.unregisterResponseCallback(activeRequestId.get());
        }
    }

    private void handleRetry(RemoteError error, int attempt) {
        if (attempt <= config.maxRemoteErrorRetries) {
            retryOperation(error);
        }
        else {
            resultBroker.communicateError(new RuntimeException(
                    String.format("Max attempts of %d for operation reached!", config.maxRemoteErrorRetries),
                    constructRemoteException(error))
            );
        }
    }

    private RemoteException constructRemoteException(RemoteError error) {
        return new RemoteException(error.getErrorClass(),
                error.getErrorMessage().isPresent() ? error.getErrorMessage().get() : "");
    }

    private void retryOperation(RemoteError remoteError) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Attempt %d failed with a retriable error", attempt), constructRemoteException(remoteError));
        }

        Runnable retry = new Runnable() {
            @Override
            public void run() {
                // TODO: need try/catch here to communicate exception if it occurs...
                executeRetry();
            }
        };

        retryExecutor.retry(retry);
    }

    private void executeRetry() {
        if (cancelled.get()) {
            // TODO: metric on this?
            return;
        }

        attempt++;
        requestToLocation(requestDetailProvider.getRetryLocation());
    }

    private void requestToLocation(HRegionLocation location) {
        Invocation invocation = requestDetailProvider.getInvocation(location);
        int requestId = sender.sendRequest(location, invocation, this);
        activeRequestId.set(requestId);
    }

    private boolean isRetryError(RemoteError error) {
        for (Class<? extends Exception> retryError : requestDetailProvider.getRemoteRetryErrors()) {
            if (retryError.getName().equals(error.getErrorClass())) {
                return true;
            }
        }

        return false;
    }
}
