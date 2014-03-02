package org.wizbang.hbase.nbhc.request;

import com.codahale.metrics.Meter;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.HbaseClientMetrics;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.HbaseOperationResultFuture;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.response.RequestResponseController;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleActionRequestInitiator {

    private final RequestSender sender;
    private final RetryExecutor retryExecutor;
    private final RequestManager requestManager;
    private final HbaseClientConfiguration config;

    public SingleActionRequestInitiator(RequestSender sender,
                                        RetryExecutor retryExecutor,
                                        RequestManager requestManager,
                                        HbaseClientConfiguration config) {
        this.sender = sender;
        this.retryExecutor = retryExecutor;
        this.requestManager = requestManager;
        this.config = config;
    }

    public <R> ListenableFuture<R> initiate(RequestDetailProvider requestDetailProvider,
                                            Function<HbaseObjectWritable, R> responseParser) {

        HbaseOperationResultFuture<R> future = new HbaseOperationResultFuture<R>();

        final SingleActionController<R> controller =new SingleActionController<R>(
                requestDetailProvider,
                future,
                responseParser,
                sender,
                retryExecutor,
                requestManager,
                config
        );

        future.setCancelCallback(new Runnable() {
            @Override
            public void run() {
                controller.cancel();
            }
        });

        controller.launch();

        return future;
    }

    private static final class SingleActionController<R> implements RequestResponseController {

        private static final Logger log = LogManager.getLogger(SingleActionController.class);

        private static final Meter ABANDONED_RETRIES_DUE_TO_CANCEL_METER = HbaseClientMetrics.meter("AbandonedRetriesDueToCancel");

        private final RequestDetailProvider requestDetailProvider;
        private final ResultBroker<R> resultBroker;
        private final Function<HbaseObjectWritable, R> responseParser;
        private final RequestSender sender;
        private final RetryExecutor retryExecutor;
        private final RequestManager requestManager;
        private final HbaseClientConfiguration config;

        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        private final AtomicInteger activeRequestId = new AtomicInteger();

        private int locationErrorCount = 0;
        private int unknownErrorCount = 0;
        private int attempt = 0;

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
            if (cancelled.get()) {
                ABANDONED_RETRIES_DUE_TO_CANCEL_METER.mark();
                return;
            }

            boolean shouldRetry;
            if (isLocationError(remoteError)) {
                locationErrorCount++;
                shouldRetry = (locationErrorCount <= config.maxLocationErrorRetries);
            }
            else {
                unknownErrorCount++;
                shouldRetry = (unknownErrorCount <= config.maxUnknownErrorRetries);
            }

            if (shouldRetry) {
                warnRemoteError(remoteError);
                retryOperation();
            }
            else {
                String message = "Max failure strikes reached " + errorsState();
                resultBroker.communicateError(new RuntimeException(message, constructRemoteException(remoteError)));
            }
        }

        private void warnRemoteError(RemoteError remoteError) {
            String errorMessage = remoteError.getErrorMessage().isPresent() ? remoteError.getErrorMessage().get() : "[none]";
            log.warn(String.format("Attempt %d failed with error class [%s] with message '%s'. Retrying. %s",
                    (attempt + 1), remoteError.getErrorClass(), errorMessage, errorsState()));
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

        private RemoteException constructRemoteException(RemoteError error) {
            return new RemoteException(error.getErrorClass(),
                    error.getErrorMessage().isPresent() ? error.getErrorMessage().get() : "");
        }

        private void retryOperation() {

            Runnable retry = new Runnable() {
                @Override
                public void run() {
                    // TODO: need try/catch here to communicate exception if it occurs...
                    executeRetry();
                }
            };

            retryExecutor.retry(retry);
        }

        private String errorsState() {
            return String.format("(location errors = %d, unknown errors = %d)", locationErrorCount, unknownErrorCount);
        }

        private void executeRetry() {
            if (cancelled.get()) {
                ABANDONED_RETRIES_DUE_TO_CANCEL_METER.mark();
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

        private boolean isLocationError(RemoteError error) {
            for (Class<? extends Exception> retryError : requestDetailProvider.getLocationErrors()) {
                if (retryError.getName().equals(error.getErrorClass())) {
                    return true;
                }
            }

            return false;
        }
    }
}
