package com.dss.backend.consensus.paxos;

import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.ProtocolType;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.SimulationMessageFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class PaxosAlgorithmTest {

    private MessageRouter mockRouter;
    private PaxosAlgorithm paxos;

    @BeforeEach
    public void setUp() {
        // Create a router mock and stub getRegisteredNodeIds() so that broadcast will send messages.
        mockRouter = mock(MessageRouter.class);
        when(mockRouter.getRegisteredNodeIds()).thenReturn(Set.of("node1", "node2", "node3"));
        paxos = new PaxosAlgorithm("node1", Arrays.asList("node1", "node2", "node3"), mockRouter);
    }

    @Test
    public void propose_GeneratesProposalNumberAndBroadcastsPrepare() {
        // Call propose with a test value.
        paxos.propose("testValue");

        // Capture the broadcasted messages.
        ArgumentCaptor<SimulationMessage> captor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeast(1)).messageSent(captor.capture());
        // There should be one ACCEPT_REQUEST (later in the flow) or PREPARE_REQUEST sent to each non-local node.
        // For the first proposal, a PREPARE_REQUEST is sent.
        boolean prepareRequestFound = captor.getAllValues().stream()
                .anyMatch(msg -> msg.getType() == MessageType.PREPARE_REQUEST);
        assertTrue(prepareRequestFound, "A PREPARE_REQUEST message should be broadcast");

        // Verify that the payload contains a proposal number (should be 1 for the first proposal)
        SimulationMessage sentMsg = captor.getAllValues().get(0);
        PaxosPayload payload = (PaxosPayload) sentMsg.getPayload();
        assertEquals(1, payload.getProposalNumber());
        assertEquals("testValue", payload.getProposedValue());
    }

    @Test
    public void handleMessage_Promise_UpdatesStateAndBroadcastsAcceptRequest() {
        // First, call propose().
        paxos.propose("initialValue");

        // Capture the proposal message to extract its proposal number.
        ArgumentCaptor<SimulationMessage> proposalCaptor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeastOnce()).messageSent(proposalCaptor.capture());
        SimulationMessage proposalMsg = proposalCaptor.getValue();
        PaxosPayload proposalPayload = (PaxosPayload) proposalMsg.getPayload();
        int proposalNumber = proposalPayload.getProposalNumber();

        // Reset interactions to clearly capture subsequent broadcast.
        reset(mockRouter);
        when(mockRouter.getRegisteredNodeIds()).thenReturn(Set.of("node1", "node2", "node3"));

        // For a three-node system, quorum is 2.
        PaxosPayload promise1 = new PaxosPayload();
        promise1.setProposalNumber(proposalNumber);
        promise1.setAcceptedId(-1);
        promise1.setAcceptedValue(null);
        SimulationMessage promiseMsg1 = SimulationMessageFactory.createMessage("node2", "node1", MessageType.PROMISE, promise1, ProtocolType.PAXOS);

        PaxosPayload promise2 = new PaxosPayload();
        promise2.setProposalNumber(proposalNumber);
        promise2.setAcceptedId(-1);
        promise2.setAcceptedValue(null);
        SimulationMessage promiseMsg2 = SimulationMessageFactory.createMessage("node3", "node1", MessageType.PROMISE, promise2, ProtocolType.PAXOS);

        paxos.handleMessage(promiseMsg1);
        paxos.handleMessage(promiseMsg2);

        // Verify that an ACCEPT_REQUEST message was sent.
        ArgumentCaptor<SimulationMessage> acceptCaptor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeastOnce()).messageSent(acceptCaptor.capture());
        boolean acceptRequestFound = acceptCaptor.getAllValues().stream()
                .anyMatch(msg -> msg.getType() == MessageType.ACCEPT_REQUEST);
        assertTrue(acceptRequestFound, "An ACCEPT_REQUEST message should be broadcast after quorum is reached");
    }

    @Test
    public void handleMessage_AcceptRequest_StoresAcceptedValue() {
        // Simulate an incoming ACCEPT_REQUEST message with proposal number 10.
        int proposalNumber = 10;
        PaxosPayload acceptRequestPayload = new PaxosPayload();
        acceptRequestPayload.setProposalNumber(proposalNumber);
        acceptRequestPayload.setProposedValue("newValue");

        SimulationMessage acceptRequestMsg = SimulationMessageFactory.createMessage("node2", "node1", MessageType.ACCEPT_REQUEST, acceptRequestPayload, ProtocolType.PAXOS);
        paxos.handleMessage(acceptRequestMsg);

        // Use reflection to verify that PaxosState's acceptedValue was updated.
        try {
            java.lang.reflect.Field paxosStateField = PaxosAlgorithm.class.getDeclaredField("paxosState");
            paxosStateField.setAccessible(true);
            Object paxosState = paxosStateField.get(paxos);

            java.lang.reflect.Method getAcceptedValueMethod = paxosState.getClass().getMethod("getAcceptedValue");
            Object acceptedValue = getAcceptedValueMethod.invoke(paxosState);
            assertEquals("newValue", acceptedValue);
        } catch (Exception e) {
            fail("Reflection failed: " + e.getMessage());
        }
    }

    @Test
    public void handleMessage_Accepted_QuorumReached_CommitsValue() {
        // Start by calling propose().
        paxos.propose("initialValue");
        ArgumentCaptor<SimulationMessage> proposalCaptor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeastOnce()).messageSent(proposalCaptor.capture());
        SimulationMessage proposalMsg = proposalCaptor.getValue();
        PaxosPayload proposalPayload = (PaxosPayload) proposalMsg.getPayload();
        int proposalNumber = proposalPayload.getProposalNumber();

        // Simulate two PROMISE responses.
        PaxosPayload promise = new PaxosPayload();
        promise.setProposalNumber(proposalNumber);
        promise.setAcceptedId(-1);
        promise.setAcceptedValue(null);
        SimulationMessage promiseMsg1 = SimulationMessageFactory.createMessage("node2", "node1", MessageType.PROMISE, promise, ProtocolType.PAXOS);
        SimulationMessage promiseMsg2 = SimulationMessageFactory.createMessage("node3", "node1", MessageType.PROMISE, promise, ProtocolType.PAXOS);
        paxos.handleMessage(promiseMsg1);
        paxos.handleMessage(promiseMsg2);

        // Simulate receiving two ACCEPTED messages.
        PaxosPayload acceptedPayload = new PaxosPayload();
        acceptedPayload.setProposalNumber(proposalNumber);
        acceptedPayload.setProposedValue("committedValue");
        SimulationMessage acceptedMsg1 = SimulationMessageFactory.createMessage("node2", "node1", MessageType.ACCEPTED, acceptedPayload, ProtocolType.PAXOS);
        SimulationMessage acceptedMsg2 = SimulationMessageFactory.createMessage("node3", "node1", MessageType.ACCEPTED, acceptedPayload, ProtocolType.PAXOS);
        paxos.handleMessage(acceptedMsg1);
        paxos.handleMessage(acceptedMsg2);

        // Verify that the PaxosState's chosen (committed) value is now set.
        try {
            java.lang.reflect.Field paxosStateField = PaxosAlgorithm.class.getDeclaredField("paxosState");
            paxosStateField.setAccessible(true);
            Object paxosState = paxosStateField.get(paxos);

            java.lang.reflect.Method getChosenValueMethod = paxosState.getClass().getMethod("getChosenValue");
            Object chosenValue = getChosenValueMethod.invoke(paxosState);
            assertEquals("committedValue", chosenValue);
        } catch (Exception e) {
            fail("Reflection failed: " + e.getMessage());
        }
    }
}