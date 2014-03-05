package org.wizbang.hbase.nbhc.request;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.RemoteErrorUtil;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.response.RequestResponseController;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang.math.RandomUtils.nextInt;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SingleActionRequestTest {

    @Mock private RequestSender sender;
    @Mock private RetryExecutor retryExecutor;
    @Mock private RequestManager manager;
    @Mock private RemoteErrorUtil remoteErrorUtil;
    private HbaseClientConfiguration config;

    @Mock private Function<HbaseObjectWritable, Integer> parser;

    private SingleActionRequestInitiator initiator;

    private static ExecutorService workerPool;

    @BeforeClass
    public static void setupWorkerPool() {
        workerPool = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void shutdownWorkerPool() {
        workerPool.shutdownNow();
    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        config = new HbaseClientConfiguration();

        initiator = new SingleActionRequestInitiator(sender, workerPool, retryExecutor, manager, remoteErrorUtil, config);
    }

    @Test
    public void testSimpleSuccessfulRequest() throws Exception {
        final Invocation invocation = mock(Invocation.class);
        final HRegionLocation location = new HRegionLocation(mock(HRegionInfo.class), randomAlphanumeric(10), nextInt(65000));
        RequestDetailProvider detail = new RequestDetailProvider() {
            @Override
            public HRegionLocation getLocation() {
                return location;
            }

            @Override
            public HRegionLocation getRetryLocation() {
                throw new UnsupportedOperationException("Should never hit this");
            }

            @Override
            public Invocation getInvocation(HRegionLocation targetLocation) {
                return invocation;
            }

            @Override
            public ImmutableSet<Class<? extends Exception>> getLocationErrors() {
                return ImmutableSet.of();
            }
        };

        int value = nextInt();

        when(parser.apply(Matchers.<HbaseObjectWritable>any())).thenReturn(value);

        final HbaseObjectWritable writable = mock(HbaseObjectWritable.class);

        ResponseExecution responseExecution = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveResponse(nextInt(), writable);
            }
        };

        when(sender.sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any()))
             .thenAnswer(senderAnswerWithResponseExecution(nextInt(), responseExecution));

        ListenableFuture<Integer> future = initiator.initiate(detail, parser);

        Integer result = future.get(30, TimeUnit.SECONDS);

        assertEquals(value, result.intValue());

        verify(sender).sendRequest(eq(location), eq(invocation), Matchers.<RequestResponseController>any());
        verify(parser).apply(writable);
    }

    @Test
    public void testErrorRetriesWithSuccess() throws Exception {
        final Invocation invocation = mock(Invocation.class);
        final HRegionLocation location = new HRegionLocation(mock(HRegionInfo.class), randomAlphanumeric(10), nextInt(65000));
        final HRegionLocation retryLocation = new HRegionLocation(mock(HRegionInfo.class), randomAlphanumeric(10), nextInt(65000));
        RequestDetailProvider detail = new RequestDetailProvider() {
            @Override
            public HRegionLocation getLocation() {
                return location;
            }

            @Override
            public HRegionLocation getRetryLocation() {
                return retryLocation;
            }

            @Override
            public Invocation getInvocation(HRegionLocation targetLocation) {
                return invocation;
            }

            @Override
            public ImmutableSet<Class<? extends Exception>> getLocationErrors() {
                return ImmutableSet.<Class<? extends Exception>>of(DummyException.class);
            }
        };

        int value = nextInt();

        when(parser.apply(Matchers.<HbaseObjectWritable>any())).thenReturn(value);

        final HbaseObjectWritable writable = mock(HbaseObjectWritable.class);

        ResponseExecution locationErrorResponse = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveRemoteError(nextInt(), new RemoteError(DummyException.class.getName(), Optional.of("location error")));
            }
        };

        ResponseExecution unknownErrorResponse = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveRemoteError(nextInt(), new RemoteError(IOException.class.getName(), Optional.of("unknown error")));
            }
        };

        ResponseExecution localErrorResponse = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveLocalError(nextInt(), new IOException("local kaboom"));
            }
        };

        ResponseExecution valueResponse = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveResponse(nextInt(), writable);
            }
        };

        when(sender.sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any()))
                .thenAnswer(senderAnswerWithResponseExecution(nextInt(), locationErrorResponse))
                .thenAnswer(senderAnswerWithResponseExecution(nextInt(), unknownErrorResponse))
                .thenAnswer(senderAnswerWithResponseExecution(nextInt(), localErrorResponse))
                .thenAnswer(senderAnswerWithResponseExecution(nextInt(), valueResponse));

        doAnswer(delayedExecutionRetry()).when(retryExecutor).retry(Matchers.<Runnable>any());

        ListenableFuture<Integer> future = initiator.initiate(detail, parser);

        Integer result = future.get(30, TimeUnit.SECONDS);

        assertEquals(value, result.intValue());

        verify(sender).sendRequest(eq(location), eq(invocation), Matchers.<RequestResponseController>any());
        verify(sender, times(3)).sendRequest(eq(retryLocation), eq(invocation), Matchers.<RequestResponseController>any());

        verify(retryExecutor, times(3)).retry(Matchers.<Runnable>any());
    }

    @Test
    public void testMaxLocationErrorsReached() throws Exception {
        ResponseExecution dummyExceptionError = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveRemoteError(nextInt(), new RemoteError(DummyException.class.getName(), Optional.<String>absent()));
            }
        };

        testMaxErrorsReached(dummyExceptionError, config.maxLocationErrorRetries);
    }

    @Test
    public void testMaxUnknownErrorsReached() throws Exception {
        ResponseExecution ioExceptionExecution = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveRemoteError(nextInt(), new RemoteError(IOException.class.getName(), Optional.of("io")));
            }
        };

        testMaxErrorsReached(ioExceptionExecution, config.maxUnknownErrorRetries);
    }

    @Test
    public void testMaxLocalErrorsReached() throws Exception {
        ResponseExecution localErrorExecution = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveLocalError(nextInt(), new RuntimeException("wat"));
            }
        };

        testMaxErrorsReached(localErrorExecution, config.maxUnknownErrorRetries);
    }

    private void testMaxErrorsReached(ResponseExecution responseExecution, int maxRetries) throws Exception {
        final Invocation invocation = mock(Invocation.class);
        final HRegionLocation location = new HRegionLocation(mock(HRegionInfo.class), randomAlphanumeric(10), nextInt(65000));
        RequestDetailProvider detail = new RequestDetailProvider() {
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
                return invocation;
            }

            @Override
            public ImmutableSet<Class<? extends Exception>> getLocationErrors() {
                return ImmutableSet.<Class<? extends Exception>>of(DummyException.class);
            }
        };

        when(sender.sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any()))
                .thenAnswer(senderAnswerWithResponseExecution(nextInt(), responseExecution));

        doAnswer(delayedExecutionRetry()).when(retryExecutor).retry(Matchers.<Runnable>any());

        ListenableFuture<Integer> future = initiator.initiate(detail, parser);

        try {
            future.get(10, TimeUnit.SECONDS);
            fail();
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertTrue(e.getCause().getMessage().contains("Max failure strikes reached"));
        }

        verify(retryExecutor, times(maxRetries)).retry(Matchers.<Runnable>any());
        verify(sender, times(maxRetries + 1))
                .sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any());
    }

    @Test
    public void testParsingException() throws Exception {
        RequestDetailProvider detail = detailNotExpectingRetries();

        when(parser.apply(Matchers.<HbaseObjectWritable>any())).thenThrow(new RuntimeException("boom"));

        final HbaseObjectWritable writable = mock(HbaseObjectWritable.class);
        ResponseExecution responseExecution = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveResponse(nextInt(), writable);
            }
        };

        when(sender.sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any()))
                .thenAnswer(senderAnswerWithResponseExecution(nextInt(), responseExecution));

        ListenableFuture<Integer> future = initiator.initiate(detail, parser);

        try {
            future.get(30, TimeUnit.SECONDS);
            fail();
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause().getMessage().equals("boom"));
        }
    }

    @Test
    public void testCancelStopsFurtherRetriesOfRemoteError() throws Exception {
        testRetryAbandonedWhenCancelBeforeResponseReceived(new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveRemoteError(nextInt(), new RemoteError(randomAlphabetic(10), Optional.<String>absent()));
            }
        });
    }

    @Test
    public void testCancelStopsFurtherRetriesOfLocalErrors() throws Exception {
        testRetryAbandonedWhenCancelBeforeResponseReceived(new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveLocalError(nextInt(), new RuntimeException("abandonment"));
            }
        });
    }

    private void testRetryAbandonedWhenCancelBeforeResponseReceived(final ResponseExecution errorExecution) throws Exception {
        RequestDetailProvider detail = detailNotExpectingRetries();

        final CountDownLatch timedOutLatch = new CountDownLatch(1);
        final CountDownLatch responseExecutedLatch = new CountDownLatch(1);
        ResponseExecution waitForTimeoutResponse = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                try {
                    timedOutLatch.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                errorExecution.respond(controller);
                responseExecutedLatch.countDown();
            }
        };

        int requestId = nextInt();
        when(sender.sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any()))
                .thenAnswer(senderAnswerWithResponseExecution(requestId, waitForTimeoutResponse));

        ListenableFuture<Integer> future = initiator.initiate(detail, parser);
        try {
            future.get(1, TimeUnit.SECONDS);
            fail();
        }
        catch (TimeoutException e) {
            // Expected
            timedOutLatch.countDown();
        }

        responseExecutedLatch.await(10, TimeUnit.SECONDS);

        verify(manager).unregisterResponseCallback(requestId);
        verifyZeroInteractions(retryExecutor);
    }

    @Test
    public void testScheduledRetryAbortedWhenCanceled() throws Exception {
        RequestDetailProvider detail = detailNotExpectingRetries();

        ResponseExecution waitForTimeoutResponse = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveRemoteError(nextInt(), new RemoteError(randomAlphabetic(10), Optional.<String>absent()));
            }
        };

        int requestId = nextInt();
        when(sender.sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any()))
                .thenAnswer(senderAnswerWithResponseExecution(requestId, waitForTimeoutResponse));

        final CountDownLatch retryInitiatedLatch = new CountDownLatch(1);
        final CountDownLatch cancelLatch = new CountDownLatch(1);
        final CountDownLatch retryDoneLatch = new CountDownLatch(1);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
                retryInitiatedLatch.countDown();
                cancelLatch.await();
                runnable.run();
                retryDoneLatch.countDown();
                return null;
            }
        }).when(retryExecutor).retry(Matchers.<Runnable>any());

        ListenableFuture<Integer> future = initiator.initiate(detail, parser);

        retryInitiatedLatch.await(10, TimeUnit.SECONDS);

        future.cancel(true);
        cancelLatch.countDown();

        retryDoneLatch.await();

        verify(manager).unregisterResponseCallback(requestId);
        verify(sender).sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any());
    }

    @Test
    public void testFatalError() throws Exception {
        RequestDetailProvider detail = detailNotExpectingRetries();

        ResponseExecution fatalErrorExecution = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveFatalError(nextInt(), new RuntimeException("fatal"));
            }
        };

        when(sender.sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any()))
            .thenAnswer(senderAnswerWithResponseExecution(nextInt(), fatalErrorExecution));

        ListenableFuture<Integer> future = initiator.initiate(detail, parser);

        try {
            future.get(10, TimeUnit.SECONDS);
            fail();
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertTrue(e.getCause().getMessage().equals("fatal"));
        }

        verify(sender).sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any());
    }

    @Test
    public void testRetryRequestSendThrowsException() throws Exception {
        RequestDetailProvider detail = mock(RequestDetailProvider.class);
        HRegionLocation location = new HRegionLocation(mock(HRegionInfo.class), randomAlphabetic(10), nextInt());

        when(detail.getLocation()).thenReturn(location);
        when(detail.getInvocation(Matchers.<HRegionLocation>any())).thenReturn(mock(Invocation.class));
        when(detail.getRetryLocation())
                .thenThrow(new RuntimeException("failure of retry loc"))
                .thenReturn(location);

        ResponseExecution errorResponse = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveLocalError(nextInt(), new RuntimeException());
            }
        };

        final int value = nextInt();
        final HbaseObjectWritable writable = mock(HbaseObjectWritable.class);
        when(parser.apply(Matchers.<HbaseObjectWritable>any())).thenReturn(value);
        ResponseExecution successResponse = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveResponse(nextInt(), writable);
            }
        };

        when(sender.sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any()))
            .thenAnswer(senderAnswerWithResponseExecution(nextInt(), errorResponse))
            .thenAnswer(senderAnswerWithResponseExecution(nextInt(), successResponse));

        doAnswer(delayedExecutionRetry()).when(retryExecutor).retry(Matchers.<Runnable>any());

        ListenableFuture<Integer> future = initiator.initiate(detail, parser);

        Integer result = future.get(10, TimeUnit.SECONDS);

        assertEquals(value, result.intValue());

        verify(sender, times(2)).sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any());
        verify(retryExecutor, times(2)).retry(Matchers.<Runnable>any());
        verify(detail, times(2)).getRetryLocation();
    }

    @Test
    public void testInitialLaunchExceptionRetries() throws Exception {
        RequestDetailProvider detail = mock(RequestDetailProvider.class);
        HRegionLocation location = new HRegionLocation(mock(HRegionInfo.class), randomAlphabetic(10), nextInt());

        when(detail.getLocation()).thenThrow(new RuntimeException("Initial loc lookup failure"));
        when(detail.getInvocation(Matchers.<HRegionLocation>any())).thenReturn(mock(Invocation.class));
        when(detail.getRetryLocation()).thenReturn(location);

        final int value = nextInt();
        final HbaseObjectWritable writable = mock(HbaseObjectWritable.class);
        when(parser.apply(Matchers.<HbaseObjectWritable>any())).thenReturn(value);
        ResponseExecution successResponse = new ResponseExecution() {
            @Override
            public void respond(RequestResponseController controller) {
                controller.receiveResponse(nextInt(), writable);
            }
        };

        when(sender.sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any()))
                .thenAnswer(senderAnswerWithResponseExecution(nextInt(), successResponse));

        doAnswer(delayedExecutionRetry()).when(retryExecutor).retry(Matchers.<Runnable>any());

        ListenableFuture<Integer> future = initiator.initiate(detail, parser);

        Integer result = future.get(10, TimeUnit.SECONDS);

        assertEquals(value, result.intValue());

        verify(sender).sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any());
        verify(retryExecutor).retry(Matchers.<Runnable>any());
        verify(detail).getLocation();
    }

    @Test
    public void testDoNotRetryRemoteError() throws Exception {
        class DummyDoNotRetry extends DoNotRetryIOException {}

        RequestDetailProvider detail = detailNotExpectingRetries();

        when(sender.sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any()))
            .thenAnswer(senderAnswerWithResponseExecution(nextInt(), new ResponseExecution() {
                @Override
                public void respond(RequestResponseController controller) {
                    controller.receiveRemoteError(nextInt(), new RemoteError(DummyDoNotRetry.class.getName(), Optional.<String>absent()));
                }
            }));

        when(remoteErrorUtil.isDoNotRetryError(Matchers.<RemoteError>any())).thenReturn(true);
        when(remoteErrorUtil.constructRemoteException(Matchers.<RemoteError>any())).thenReturn(new RemoteException(DummyDoNotRetry.class.getName(), ""));

        ListenableFuture<Integer> future = initiator.initiate(detail, parser);

        try {
            future.get(10, TimeUnit.SECONDS);
            fail();
        }
        catch (ExecutionException e) {
            // Expected
        }

        ArgumentCaptor<RemoteError> errorCaptor = ArgumentCaptor.forClass(RemoteError.class);
        verify(remoteErrorUtil).isDoNotRetryError(errorCaptor.capture());
        assertEquals(DummyDoNotRetry.class.getName(), errorCaptor.getValue().getErrorClass());
        verify(sender).sendRequest(Matchers.<HRegionLocation>any(), Matchers.<Invocation>any(), Matchers.<RequestResponseController>any());
    }

    private RequestDetailProvider detailNotExpectingRetries() {
        return new RequestDetailProvider() {
            @Override
            public HRegionLocation getLocation() {
                return new HRegionLocation(mock(HRegionInfo.class), randomAlphanumeric(10), nextInt(65000));
            }

            @Override
            public HRegionLocation getRetryLocation() {
                throw new UnsupportedOperationException("Should never hit this");
            }

            @Override
            public Invocation getInvocation(HRegionLocation targetLocation) {
                return mock(Invocation.class);
            }

            @Override
            public ImmutableSet<Class<? extends Exception>> getLocationErrors() {
                return ImmutableSet.of();
            }
        };
    }

    private Answer delayedExecutionRetry() {
        return new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
                Executors.newSingleThreadScheduledExecutor().schedule(runnable, 50L, TimeUnit.MILLISECONDS);
                return null;
            }
        };
    }

    private static interface ResponseExecution {
        void respond(RequestResponseController controller);
    }

    private Answer<Integer> senderAnswerWithResponseExecution(final int requestId,
                                                              final ResponseExecution responseExecution) {
        return new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
                final RequestResponseController controller = (RequestResponseController) invocationOnMock.getArguments()[2];
                Executors.newSingleThreadScheduledExecutor().schedule(new Runnable() {
                    @Override
                    public void run() {
                        responseExecution.respond(controller);
                    }
                }, 50L, TimeUnit.MILLISECONDS);

                return requestId;
            }
        };
    }

    private static final class DummyException extends Exception {}
}
