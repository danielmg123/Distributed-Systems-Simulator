package com.dss.backend.consensus.paxos;

import com.dss.backend.engine.DefaultScheduler;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.ProtocolType;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.SimulationMessageFactory;
import com.dss.backend.messaging.VirtualNode;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

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
    public void getRoleLabel_returnsAcceptor() {
        // Basic Paxos is leaderless -- every node is an acceptor.
        assertEquals("ACCEPTOR", paxos.getRoleLabel());
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

        // Verify that the payload contains a proposal number. node1 is index 0 of 3 nodes,
        // and proposal numbers are now salted as (round * clusterSize + nodeIndex), so the
        // first proposal from node1 is (1 * 3 + 0) = 3, not a plain "1". See
        // PaxosAlgorithm.generateNextProposalNumber() for why: a plain per-node counter would
        // let two different nodes generate colliding proposal numbers.
        SimulationMessage sentMsg = captor.getAllValues().get(0);
        PaxosPayload payload = (PaxosPayload) sentMsg.getPayload();
        assertEquals(3, payload.getProposalNumber());
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

    @Test
    public void generateNextProposalNumber_AcrossConcurrentProposers_NeverCollide() {
        // Two nodes in the same 3-node cluster, each acting as a proposer.
        // Before the node-index salting fix, both would independently generate
        // 1, 2, 3, ... and collide on every round, which breaks Paxos safety
        // (acceptors compare proposal numbers as plain integers across all
        // proposers, so colliding numbers from different nodes are indistinguishable).
        MessageRouter router1 = mock(MessageRouter.class);
        when(router1.getRegisteredNodeIds()).thenReturn(Set.of("node1", "node2", "node3"));
        PaxosAlgorithm node1Paxos = new PaxosAlgorithm("node1", Arrays.asList("node1", "node2", "node3"), router1);

        MessageRouter router2 = mock(MessageRouter.class);
        when(router2.getRegisteredNodeIds()).thenReturn(Set.of("node1", "node2", "node3"));
        PaxosAlgorithm node2Paxos = new PaxosAlgorithm("node2", Arrays.asList("node1", "node2", "node3"), router2);

        for (int i = 0; i < 10; i++) {
            node1Paxos.propose("node1-value-" + i);
            node2Paxos.propose("node2-value-" + i);
        }

        Set<Integer> node1Numbers = collectPrepareProposalNumbers(router1);
        Set<Integer> node2Numbers = collectPrepareProposalNumbers(router2);

        assertEquals(10, node1Numbers.size(), "node1 should have generated 10 distinct proposal numbers");
        assertEquals(10, node2Numbers.size(), "node2 should have generated 10 distinct proposal numbers");

        Set<Integer> intersection = new HashSet<>(node1Numbers);
        intersection.retainAll(node2Numbers);
        assertTrue(intersection.isEmpty(),
                "node1 and node2 must never generate the same proposal number, but both used: " + intersection);
    }

    private Set<Integer> collectPrepareProposalNumbers(MessageRouter router) {
        ArgumentCaptor<SimulationMessage> captor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(router, atLeastOnce()).messageSent(captor.capture());
        Set<Integer> numbers = new HashSet<>();
        for (SimulationMessage msg : captor.getAllValues()) {
            if (msg.getType() == MessageType.PREPARE_REQUEST) {
                numbers.add(((PaxosPayload) msg.getPayload()).getProposalNumber());
            }
        }
        return numbers;
    }

    @Test
    public void onAccepted_QuorumReached_BroadcastsCommitSoAllNodesLearnChosenValue() throws Exception {
        // Real MessageRouter + 3 real PaxosAlgorithm instances, each wrapped in a real
        // VirtualNode so messages actually flow asynchronously between nodes, not mocked.
        // This is the regression test for the missing Learner phase: before wiring
        // broadcastCommit() into onAccepted(), only the proposer's PaxosState ever got
        // a chosenValue -- the other two acceptors had no way to learn the outcome.
        MessageRouter router = new MessageRouter();
        List<String> nodeIds = Arrays.asList("node1", "node2", "node3");

        PaxosAlgorithm node1Algorithm = new PaxosAlgorithm("node1", nodeIds, router);
        PaxosAlgorithm node2Algorithm = new PaxosAlgorithm("node2", nodeIds, router);
        PaxosAlgorithm node3Algorithm = new PaxosAlgorithm("node3", nodeIds, router);

        List<VirtualNode> virtualNodes = new ArrayList<>();
        virtualNodes.add(startVirtualNode("node1", node1Algorithm, router));
        virtualNodes.add(startVirtualNode("node2", node2Algorithm, router));
        virtualNodes.add(startVirtualNode("node3", node3Algorithm, router));

        try {
            node1Algorithm.propose("converged-value");

            // Poll with a bounded timeout rather than a fixed sleep, since delivery is async.
            Object node1Chosen = null;
            Object node2Chosen = null;
            Object node3Chosen = null;
            for (int i = 0; i < 50; i++) {
                node1Chosen = getChosenValue(node1Algorithm);
                node2Chosen = getChosenValue(node2Algorithm);
                node3Chosen = getChosenValue(node3Algorithm);
                if (node1Chosen != null && node2Chosen != null && node3Chosen != null) {
                    break;
                }
                Thread.sleep(100);
            }

            assertEquals("converged-value", node1Chosen, "proposer should have committed the value");
            assertEquals("converged-value", node2Chosen,
                    "node2 (acceptor) should have learned the chosen value via the COMMIT broadcast");
            assertEquals("converged-value", node3Chosen,
                    "node3 (acceptor) should have learned the chosen value via the COMMIT broadcast");
        } finally {
            virtualNodes.forEach(VirtualNode::stop);
        }
    }

    private VirtualNode startVirtualNode(String nodeId, PaxosAlgorithm algorithm, MessageRouter router) {
        Node node = new Node();
        node.setId(nodeId);
        node.setStatus(NodeStatus.ACTIVE);
        VirtualNode vNode = new VirtualNode(node, algorithm, router,
                new DefaultScheduler(Executors.newSingleThreadScheduledExecutor()));
        vNode.start();
        router.registerNode(nodeId, vNode);
        return vNode;
    }

    private Object getChosenValue(PaxosAlgorithm paxos) throws Exception {
        java.lang.reflect.Field paxosStateField = PaxosAlgorithm.class.getDeclaredField("paxosState");
        paxosStateField.setAccessible(true);
        Object paxosState = paxosStateField.get(paxos);
        java.lang.reflect.Method getChosenValueMethod = paxosState.getClass().getMethod("getChosenValue");
        return getChosenValueMethod.invoke(paxosState);
    }
}