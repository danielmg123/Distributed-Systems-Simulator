package com.dss.backend.consensus.zab;

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

public class ZabAlgorithmTest {

    private MessageRouter mockRouter;
    private Zab zab;

    @BeforeEach
    public void setUp() {
        mockRouter = mock(MessageRouter.class);
        when(mockRouter.getRegisteredNodeIds()).thenReturn(Set.of("node1", "node2", "node3"));
        zab = new Zab();
        zab.setMessageRouter(mockRouter);
        zab.setLeader(true);
        zab.setNodeId("node1");
        zab.setTotalNodes(3);
    }

    @Test
    public void propose_LeaderBroadcastsProposalAndWaitsForAcks() {
        zab.propose("proposedValue");

        // Capture the PROPOSAL message broadcast.
        ArgumentCaptor<SimulationMessage> captor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeastOnce()).messageSent(captor.capture());
        SimulationMessage sentMsg = captor.getAllValues().stream()
                .filter(msg -> msg.getType() == MessageType.PROPOSAL)
                .findFirst().orElse(null);
        assertNotNull(sentMsg, "A PROPOSAL message should be broadcast");
        ZabPayload payload = (ZabPayload) sentMsg.getPayload();
        assertTrue(payload.getZxid() > 0);
        assertEquals("proposedValue", payload.getProposedValue());
    }

    @Test
    public void handleMessage_Proposal_FollowerSendsAck() {
        // Set node as follower.
        zab.setLeader(false);
        // Create a PROPOSAL message.
        ZabPayload proposalPayload = new ZabPayload(MessageType.PROPOSAL, 1, "value1");
        SimulationMessage proposalMsg = SimulationMessageFactory.createMessage("node1", "node2", MessageType.PROPOSAL, proposalPayload, ProtocolType.ZAB);

        zab.handleMessage(proposalMsg);

        // Verify that an ACK message is sent in response.
        ArgumentCaptor<SimulationMessage> captor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeastOnce()).messageSent(captor.capture());
        SimulationMessage ackMsg = captor.getAllValues().stream()
                .filter(msg -> msg.getType() == MessageType.ACK)
                .findFirst().orElse(null);
        assertNotNull(ackMsg, "An ACK message should be sent by a follower upon receiving a PROPOSAL");
        ZabPayload ackPayload = (ZabPayload) ackMsg.getPayload();
        assertEquals(1, ackPayload.getZxid());
        assertEquals("value1", ackPayload.getProposedValue());
    }

    @Test
    public void handleMessage_Ack_LeaderCommitsWhenQuorum() {
        // Set node as leader.
        zab.setLeader(true);
        // Propose a value.
        zab.propose("valueToCommit");

        // Capture the proposal to extract zxid.
        ArgumentCaptor<SimulationMessage> captor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeastOnce()).messageSent(captor.capture());
        SimulationMessage proposalMsg = captor.getAllValues().stream()
                .filter(msg -> msg.getType() == MessageType.PROPOSAL)
                .findFirst().orElse(null);
        assertNotNull(proposalMsg, "A PROPOSAL message should be broadcast");
        ZabPayload proposalPayload = (ZabPayload) proposalMsg.getPayload();
        long zxid = proposalPayload.getZxid();

        // Clear previous interactions.
        reset(mockRouter);
        when(mockRouter.getRegisteredNodeIds()).thenReturn(Set.of("node1", "node2", "node3"));

        // Simulate receiving two ACK messages to reach quorum (for 3 nodes, quorum = 2).
        ZabPayload ackPayload = new ZabPayload(MessageType.ACK, zxid, "valueToCommit");
        SimulationMessage ackMsg1 = SimulationMessageFactory.createMessage("node2", "node1", MessageType.ACK, ackPayload, ProtocolType.ZAB);
        SimulationMessage ackMsg2 = SimulationMessageFactory.createMessage("node3", "node1", MessageType.ACK, ackPayload, ProtocolType.ZAB);
        zab.handleMessage(ackMsg1);
        zab.handleMessage(ackMsg2);

        // Verify that a COMMIT message was broadcast.
        ArgumentCaptor<SimulationMessage> commitCaptor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeastOnce()).messageSent(commitCaptor.capture());
        boolean commitFound = commitCaptor.getAllValues().stream()
                .anyMatch(msg -> msg.getType() == MessageType.COMMIT);
        assertTrue(commitFound, "Leader should broadcast COMMIT when quorum of ACKs is reached");
    }
}