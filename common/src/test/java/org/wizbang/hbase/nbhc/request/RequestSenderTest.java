package org.wizbang.hbase.nbhc.request;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.wizbang.hbase.nbhc.dispatch.RegionServerDispatcher;
import org.wizbang.hbase.nbhc.dispatch.Request;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.response.RequestResponseController;

import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang.math.RandomUtils.nextInt;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class RequestSenderTest {

    private RequestManager manager;
    private RegionServerDispatcher dispatcher;

    private RequestSender sender;

    @Before
    public void setUp() throws Exception {
        manager = mock(RequestManager.class);
        dispatcher = mock(RegionServerDispatcher.class);

        sender = new RequestSender(manager, dispatcher);
    }

    @Test
    public void testSendToLocation() throws Exception {
        HostAndPort host = HostAndPort.fromParts(randomAlphabetic(10), nextInt(60000));
        Invocation invocation = mock(Invocation.class);
        RequestResponseController controller = mock(RequestResponseController.class);

        int id = nextInt();
        when(manager.registerController(Matchers.<RequestResponseController>any())).thenReturn(id);

        int result = sender.sendRequest(host, invocation, controller);

        verify(manager).registerController(controller);
        ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
        verify(dispatcher).request(eq(host), requestCaptor.capture());

        assertEquals(invocation, requestCaptor.getValue().getInvocation());
        assertEquals(id, result);
        assertEquals(id, requestCaptor.getValue().getRequestId());
    }

    @Test
    public void testSendToHost() throws Exception {
        HostAndPort host = HostAndPort.fromParts(randomAlphabetic(10), nextInt(60000));
        Invocation invocation = mock(Invocation.class);
        RequestResponseController controller = mock(RequestResponseController.class);

        int id = nextInt();
        when(manager.registerController(Matchers.<RequestResponseController>any())).thenReturn(id);

        int result = sender.sendRequest(host, invocation, controller);

        verify(manager).registerController(controller);
        ArgumentCaptor<HostAndPort> hostCaptor = ArgumentCaptor.forClass(HostAndPort.class);
        ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
        verify(dispatcher).request(hostCaptor.capture(), requestCaptor.capture());

        assertEquals(host, hostCaptor.getValue());
        assertEquals(invocation, requestCaptor.getValue().getInvocation());
        assertEquals(id, result);
        assertEquals(id, requestCaptor.getValue().getRequestId());
    }
}
