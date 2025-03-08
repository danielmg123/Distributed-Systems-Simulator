package com.dss.backend.consensus.view_stamped_replication;

import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.ProtocolType;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.SimulationMessageFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ViewStampedReplicationTest {

    private MessageRouter mockRouter;
    private ViewStampedReplication vsr;

    @BeforeEach
    public void setUp() {
        mockRouter = mock(MessageRouter.class);
        when(mockRouter.getRegisteredNodeIds()).thenReturn(Set.of("node1", "node2", "node3"));
        vsr = new ViewStampedReplication();
        vsr.setMessageRouter(mockRouter);
        vsr.setNodeId("node1");
        vsr.initBroadcaster();
        // Assume totalNodes is set externally if needed.
    }

    @Test
    public void propose_PrimaryNodeBroadcastsPrepare() {
        // Set as primary.
        vsr.setPrimary(true);
        // Call propose with a test value.
        vsr.propose("prepValue");

        // Verify that a PREPARE message was broadcast.
        ArgumentCaptor<SimulationMessage> captor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeastOnce()).messageSent(captor.capture());
        SimulationMessage sentMsg = captor.getAllValues().stream()
                .filter(msg -> msg.getType() == MessageType.PREPARE)
                .findFirst().orElse(null);
        assertNotNull(sentMsg, "A PREPARE message should be broadcast");
        VsrPayload payload = (VsrPayload) sentMsg.getPayload();
        assertEquals("prepValue", payload.getProposedValue());

        // Verify that the internal pendingOps map contains the proposal.
        try {
            java.lang.reflect.Field pendingOpsField = ViewStampedReplication.class.getDeclaredField("pendingOps");
            pendingOpsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.Map<Integer, Object> pendingOps = (java.util.Map<Integer, Object>) pendingOpsField.get(vsr);
            assertTrue(pendingOps.containsValue("prepValue"));
        } catch (Exception e) {
            fail("Reflection failed: " + e.getMessage());
        }
    }

    @Test
    public void handleMessage_PrepareResponse_QuorumReached_Commits() {
        // Set as primary.
        vsr.setPrimary(true);
        vsr.setNodeId("node1");
        vsr.initBroadcaster();
        // Propose a value.
        vsr.propose("valueX");

        // Capture the op number from the broadcasted PREPARE.
        ArgumentCaptor<SimulationMessage> captor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeastOnce()).messageSent(captor.capture());
        SimulationMessage prepMsg = captor.getAllValues().stream()
                .filter(msg -> msg.getType() == MessageType.PREPARE)
                .findFirst().orElse(null);
        assertNotNull(prepMsg, "A PREPARE message should be broadcast");
        VsrPayload prepPayload = (VsrPayload) prepMsg.getPayload();
        int opNum = prepPayload.getOpNum();

        // Clear router interactions.
        reset(mockRouter);
        when(mockRouter.getRegisteredNodeIds()).thenReturn(Set.of("node1", "node2", "node3"));

        // Simulate receiving two PREPARE_RESPONSE messages from two different nodes.
        VsrPayload responsePayload = new VsrPayload(MessageType.PREPARE_RESPONSE, 0, opNum, "valueX");
        SimulationMessage responseMsg1 = SimulationMessageFactory.createMessage("node2", "node1", MessageType.PREPARE_RESPONSE, responsePayload, ProtocolType.VIEW_STAMPED_REPLICATION);
        SimulationMessage responseMsg2 = SimulationMessageFactory.createMessage("node3", "node1", MessageType.PREPARE_RESPONSE, responsePayload, ProtocolType.VIEW_STAMPED_REPLICATION);
        vsr.handleMessage(responseMsg1);
        vsr.handleMessage(responseMsg2);

        // Verify that a COMMIT message was broadcast.
        ArgumentCaptor<SimulationMessage> commitCaptor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeastOnce()).messageSent(commitCaptor.capture());
        boolean commitFound = commitCaptor.getAllValues().stream()
                .anyMatch(msg -> msg.getType() == MessageType.COMMIT);
        assertTrue(commitFound, "A COMMIT message should be broadcast when quorum of PREPARE_RESPONSE is reached");
    }
}