package org.wizbang.hbase.nbhc.dispatch;

import com.google.common.base.Optional;
import org.junit.Before;
import org.junit.Test;
import org.wizbang.hbase.nbhc.response.RequestResponseController;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class RequestManagerTest {

    private RequestManager manager;

    @Before
    public void setUp() throws Exception {
        manager = new RequestManager();
    }

    @Test
    public void testRegisterAndRetrieve() throws Exception {
        RequestResponseController controller = mock(RequestResponseController.class);

        int id = manager.registerController(controller);
        Optional<RequestResponseController> retrieved = manager.retrieveCallback(id);

        assertTrue(retrieved.isPresent());
        assertEquals(controller, retrieved.get());

        // Second retrieval should find nothing
        assertFalse(manager.retrieveCallback(id).isPresent());
    }

    @Test
    public void testRemovedOnUnregister() throws Exception {
        RequestResponseController controller = mock(RequestResponseController.class);

        int id = manager.registerController(controller);
        manager.unregisterResponseCallback(id);

        assertFalse(manager.retrieveCallback(id).isPresent());
    }
}
