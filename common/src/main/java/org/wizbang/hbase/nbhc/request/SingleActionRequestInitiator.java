package org.wizbang.hbase.nbhc.request;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.HbaseClientMetrics;
import org.wizbang.hbase.nbhc.RemoteErrorUtil;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.HbaseOperationResultFuture;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.response.RequestResponseController;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleActionRequestInitiator {

    private static final Timer LAUNCH_DELAY_TIMER = HbaseClientMetrics.timer("Initiator:SingleAction:LaunchDelay");
    private static final Meter LOCAL_ERRORS_METER = HbaseClientMetrics.meter("Initiator:SingleAction:LocalErrors");
    private static final Meter REMOTE_LOCATION_ERRORS_METER = HbaseClientMetrics.meter("Initiator:SingleAction:RemoteLocationErrors");
    private static final Meter REMOTE_UNKNOWN_ERRORS_METER = HbaseClientMetrics.meter("Initiator:SingleAction:RemoteUnknownErrors");
    private static final Meter FATAL_ERRORS_METER = HbaseClientMetrics.meter("Initiator:SingleAction:FatalErrors");
    private static final Meter FULL_FAILURES_METER = HbaseClientMetrics.meter("Initiator:SingleAction:FullFailures");
    private static final Histogram LOCATION_ERRORS_PER_FULFILLED_REQUEST = HbaseClientMetrics.histogram("Initiator:SingleAction:LocationErrorsPerFulfilledRequest");
    private static final Histogram UNKNOWN_ERRORS_PER_FULFILLED_REQUEST = HbaseClientMetrics.histogram("Initiator:SingleAction:UnknownErrorsPerFulfilledRequest");
    private static final Timer TIME_TO_FULFILL_REQUEST_TIMER = HbaseClientMetrics.timer("Initiator:SingleAction:TimeToFulfill");

    private final RequestSender sender;
    private final RetryExecutor retryExecutor;
    private final RequestManager requestManager;
    private final ExecutorService workerPool;
    private final RemoteErrorUtil remoteErrorUtil;
    private final HbaseClientConfiguration config;

    public SingleActionRequestInitiator(RequestSender sender,
                                        ExecutorService workerPool,
                                        RetryExecutor retryExecutor,
                                        RequestManager requestManager,
                                        RemoteErrorUtil remoteErrorUtil,
                                        HbaseClientConfiguration config) {
        this.sender = sender;
        this.retryExecutor = retryExecutor;
        this.requestManager = requestManager;
        this.workerPool = workerPool;
        this.remoteErrorUtil = remoteErrorUtil;
        this.config = config;
    }

    public <R> ListenableFuture<R> initiate(RequestDetailProvider requestDetailProvider,
                                            Function<HbaseObjectWritable, R> responseParser) {

        HbaseOperationResultFuture<R> future = new HbaseOperationResultFuture<R>();

        final SingleActionController<R> controller =new SingleActionController<R>(
                requestDetailProvider,
                future,
                responseParser
        );

        future.setCancelCallback(new Runnable() {
            @Override
            public void run() {
                controller.cancel();
            }
        });

        final long start = System.currentTimeMillis();
        workerPool.submit(new Runnable() {
            @Override
            public void run() {
                LAUNCH_DELAY_TIMER.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                controller.launch();
            }
        });

        return future;
    }

    private static final Logger log = LogManager.getLogger(SingleActionController.class);

    private static final Meter ABANDONED_RETRIES_DUE_TO_CANCEL_METER = HbaseClientMetrics.meter("Controller:SingleAction:AbandonedRetriesDueToCancel");

    private final class SingleActionController<R> implements RequestResponseController {

        private final RequestDetailProvider requestDetailProvider;
        private final ResultBroker<R> resultBroker;
        private final Function<HbaseObjectWritable, R> responseParser;

        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        private final AtomicInteger activeRequestId = new AtomicInteger();

        private int locationErrorCount = 0;
        private int unknownErrorCount = 0;
        private int attempt = 0;

        private long launchTimestamp;

        private SingleActionController(RequestDetailProvider requestDetailProvider,
                                       ResultBroker<R> resultBroker,
                                       Function<HbaseObjectWritable, R> responseParser) {
            this.requestDetailProvider = requestDetailProvider;
            this.resultBroker = resultBroker;
            this.responseParser = responseParser;
        }

        public void launch() {
            launchTimestamp = System.currentTimeMillis();
            try {
                requestToLocation(requestDetailProvider.getLocation());
            }
            catch (Exception e) {
                handleLocalError(e);
            }
        }

        private void success(R result) {
            LOCATION_ERRORS_PER_FULFILLED_REQUEST.update(locationErrorCount);
            UNKNOWN_ERRORS_PER_FULFILLED_REQUEST.update(unknownErrorCount);
            TIME_TO_FULFILL_REQUEST_TIMER.update(System.currentTimeMillis() - launchTimestamp, TimeUnit.MILLISECONDS);

            resultBroker.communicateResult(result);
        }

        private void failure(Throwable error) {
            FULL_FAILURES_METER.mark();

            resultBroker.communicateError(error);
        }

        @Override
        public void receiveResponse(int requestId, HbaseObjectWritable value) {
            try {
                R result = responseParser.apply(value);
                success(result);
            }
            catch (Exception e) {
                failure(e);
            }
        }

        @Override
        public void receiveRemoteError(int requestId, RemoteError remoteError) {
            if (ignoreDueToCancel()) {
                return;
            }

            if (remoteErrorUtil.isDoNotRetryError(remoteError)) {
                failure(remoteErrorUtil.constructRemoteException(remoteError));
                return;
            }

            boolean shouldRetry = shouldRetryRemoteError(remoteError);
            if (shouldRetry) {
                warnRemoteError(remoteError);
                retryOperation();
            }
            else {
                failDueToMaxStrikes(remoteErrorUtil.constructRemoteException(remoteError));
            }
        }

        @Override
        public void receiveLocalError(int requestId, Throwable error) {
            handleLocalError(error);
        }

        @Override
        public void receiveFatalError(int requestId, Throwable error) {
            FATAL_ERRORS_METER.mark();
            failure(error);
        }

        @Override
        public void cancel() {
            if (cancelled.compareAndSet(false, true)) {
                requestManager.unregisterResponseCallback(activeRequestId.get());
            }
        }

        private void retryOperation() {

            Runnable retry = new Runnable() {
                @Override
                public void run() {
                    try {
                        executeRetry();
                    }
                    catch (Throwable e) {
                        handleLocalError(e);
                    }
                }
            };

            retryExecutor.retry(retry);
        }

        private boolean ignoreDueToCancel() {
            if (cancelled.get()) {
                ABANDONED_RETRIES_DUE_TO_CANCEL_METER.mark();
                return true;
            }

            return false;
        }

        private void executeRetry() {
            if (ignoreDueToCancel()) {
                return;
            }

            attempt++;
            requestToLocation(requestDetailProvider.getRetryLocation());
        }

        private void failDueToMaxStrikes(Throwable doomingError) {
            String message = "Max failure strikes reached " + getErrorsState();
            failure(new RuntimeException(message, doomingError));
        }

        private void handleLocalError(Throwable error) {
            LOCAL_ERRORS_METER.mark();
            if (ignoreDueToCancel()) {
                return;
            }

            if (shouldRetryUnknownError()) {
                log.warn(String.format("Attempt %d failed due to local communication error. Retrying. %s",
                        (attempt + 1), getErrorsState()), error);
                retryOperation();
            }
            else {
                failDueToMaxStrikes(error);
            }
        }

        private boolean shouldRetryRemoteError(RemoteError remoteError) {
            if (isLocationError(remoteError)) {
                REMOTE_LOCATION_ERRORS_METER.mark();
                locationErrorCount++;
                return (locationErrorCount <= config.maxLocationErrorRetries);
            }

            REMOTE_UNKNOWN_ERRORS_METER.mark();
            return shouldRetryUnknownError();
        }

        private boolean shouldRetryUnknownError() {
            unknownErrorCount++;
            return (unknownErrorCount <= config.maxUnknownErrorRetries);
        }

        private void warnRemoteError(RemoteError remoteError) {
            String errorMessage = remoteError.getErrorMessage().isPresent() ? remoteError.getErrorMessage().get() : "[none]";
            log.warn(String.format("Attempt %d failed with error class [%s] with message '%s'. Retrying. %s",
                    (attempt + 1), remoteError.getErrorClass(), errorMessage, getErrorsState()));
        }

        private void requestToLocation(HRegionLocation location) {
            Invocation invocation = requestDetailProvider.getInvocation(location);
            HostAndPort host = HostAndPort.fromParts(location.getHostname(), location.getPort());
            int requestId = sender.sendRequest(host, invocation, this);
            activeRequestId.set(requestId);
        }

        private String getErrorsState() {
            return String.format("(location errors = %d, unknown errors = %d)", locationErrorCount, unknownErrorCount);
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
