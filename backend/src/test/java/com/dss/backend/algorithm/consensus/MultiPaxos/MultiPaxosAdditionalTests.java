package com.dss.backend.algorithm.consensus.MultiPaxos;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.multi_paxos.MultiPaxos;
import com.dss.backend.algorithm.consensus.paxos.PaxosPayload;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.VirtualNodeThread;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

// This test class adds extra tests for MultiPaxos covering non-leader proposals,
// conflicting proposals, and recovery after node failure.
public class MultiPaxosAdditionalTests {

    private MultiPaxos leader;
    private MessageRouter router;

    // --- Dummy implementations for testing ---

    // A dummy ConsensusAlgorithm that does nothing (needed to construct dummy VirtualNodeThread instances).
    static class DummyConsensusAlgorithm implements ConsensusAlgorithm {
        @Override
        public void propose(Object value) { }
        @Override
        public boolean accept(Object proposal) { return true; }
        @Override
        public void commit(Object value) { }
        @Override
        public void handleMessage(SimulationMessage msg) { }
    }

    // A dummy VirtualNodeThread that can simulate failure and recovery.
    static class DummyVirtualNode extends VirtualNodeThread {
        public DummyVirtualNode(String nodeId) {
            super(createDummyNode(nodeId), new DummyConsensusAlgorithm(), new MessageRouter());
        }
        private static Node createDummyNode(String nodeId) {
            Node node = new Node();
            node.setId(nodeId);
            node.setStatus(NodeStatus.ACTIVE);
            return node;
        }
        @Override
        public void run() {
            // For testing purposes, we do not need any background processing.
        }
    }

    @BeforeEach
    public void setUp() {
        router = new MessageRouter();
        // Create a leader instance of MultiPaxos and configure it.
        leader = new MultiPaxos();
        leader.setLeader(true);
        leader.setTotalNodes(3);
        leader.setMessageRouter(router);
        // Inject a scheduler for controlling timeouts during tests.
        leader.setScheduler(Executors.newSingleThreadScheduledExecutor());
        // Register three dummy nodes in the router.
        router.registerNode("node1", new DummyVirtualNode("node1"));
        router.registerNode("node2", new DummyVirtualNode("node2"));
        router.registerNode("node3", new DummyVirtualNode("node3"));
    }

    // Test that a non-leader does not initiate consensus.
    @Test
    public void testProposalFromNonLeader() {
        MultiPaxos nonLeader = new MultiPaxos();
        nonLeader.setLeader(false); // Mark as non-leader
        nonLeader.setTotalNodes(3);
        nonLeader.setMessageRouter(router);

        nonLeader.propose("nonLeaderValue");

        // Expect that no prepare phase is started because node is not leader.
        assertFalse(nonLeader.isPreparePhaseCompleted(),
                "Non-leader should not complete the prepare phase");
        // And the proposal counter remains zero.
        assertEquals(0, nonLeader.getCurrentProposalNumber(),
                "Non-leader should not increment proposal counter");
    }

    // Test conflicting proposals. Here, the leader initially proposes "value1" but then
    // receives a promise from one node that indicates it had previously accepted "value2" with a higher ID.
    // The leader should then adopt "value2" when sending its ACCEPT_REQUEST.
    @Test
    public void testConflictingProposals() {
        // Leader proposes initial value "value1"
        leader.propose("value1");
        int currentProposal = leader.getCurrentProposalNumber();

        // Simulate promise responses:
        // Node1’s promise includes an already accepted proposal with a higher acceptedId.
        PaxosPayload promise1 = new PaxosPayload();
        promise1.setProposalNumber(currentProposal);
        promise1.setAcceptedId(10);
        promise1.setAcceptedValue("value2");

        // Node2’s promise indicates no previously accepted proposal.
        PaxosPayload promise2 = new PaxosPayload();
        promise2.setProposalNumber(currentProposal);
        promise2.setAcceptedId(-1);
        promise2.setAcceptedValue(null);

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.PROMISE, promise1));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.PROMISE, promise2));

        // After quorum is reached, the prepare phase should be complete.
        assertTrue(leader.isPreparePhaseCompleted(),
                "Prepare phase should complete after quorum is reached");

        // Now simulate ACCEPTED responses from node1 and node2
        PaxosPayload acceptedPayload = new PaxosPayload();
        acceptedPayload.setProposalNumber(currentProposal);
        // Since one promise carried an accepted proposal ("value2"), that value should be adopted.
        acceptedPayload.setProposedValue("value2");

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.ACCEPTED, acceptedPayload));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.ACCEPTED, acceptedPayload));

        // Finally, the committed value should be "value2"
        assertEquals("value2", leader.getCommittedValue(),
                "Committed value should match the highest accepted proposal from promises");
    }

    // Test recovery after node failure.
    // First, simulate a failure of node2 (so it does not respond).
    // Then, after the first proposal completes using responses from node1 and node3,
    // mark node2 as recovered and run another proposal.
    @Test
    public void testRecoveryAfterNodeFailure() {
        // Simulate node2 failure.
        DummyVirtualNode failedNode2 = new DummyVirtualNode("node2");
        failedNode2.failNode(); // Set its status to FAILED.
        router.registerNode("node2", failedNode2);

        // Leader proposes a value while node2 is down.
        leader.propose("recoveryTest");
        int currentProposal = leader.getCurrentProposalNumber();

        // Simulate promise responses from node1 and node3 only.
        PaxosPayload promise1 = new PaxosPayload();
        promise1.setProposalNumber(currentProposal);
        promise1.setAcceptedId(-1);
        promise1.setAcceptedValue(null);

        PaxosPayload promise3 = new PaxosPayload();
        promise3.setProposalNumber(currentProposal);
        promise3.setAcceptedId(-1);
        promise3.setAcceptedValue(null);

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.PROMISE, promise1));
        leader.handleMessage(new SimulationMessage("node3", "self", MessageType.PROMISE, promise3));

        // For a 3‑node cluster, quorum is 2, so prepare phase should complete.
        assertTrue(leader.isPreparePhaseCompleted(),
                "Prepare phase should complete with responses from node1 and node3 even if node2 is down");

        // Simulate ACCEPTED responses from node1 and node3.
        PaxosPayload acceptedPayload = new PaxosPayload();
        acceptedPayload.setProposalNumber(currentProposal);
        acceptedPayload.setProposedValue("recoveryTest");

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.ACCEPTED, acceptedPayload));
        leader.handleMessage(new SimulationMessage("node3", "self", MessageType.ACCEPTED, acceptedPayload));
        assertEquals("recoveryTest", leader.getCommittedValue(),
                "Committed value should match the proposal even with one node down");

        // Now simulate recovery of node2.
        failedNode2.recoverNode();
        // For a new proposal, node2 (now recovered) should participate.
        leader.propose("newValueAfterRecovery");
        int newProposal = leader.getCurrentProposalNumber();

        PaxosPayload promiseRecovered = new PaxosPayload();
        promiseRecovered.setProposalNumber(newProposal);
        promiseRecovered.setAcceptedId(-1);
        promiseRecovered.setAcceptedValue(null);

        // Simulate promise responses from node1 and the recovered node2.
        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.PROMISE, promiseRecovered));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.PROMISE, promiseRecovered));
        assertTrue(leader.isPreparePhaseCompleted(),
                "Prepare phase should complete after node2 recovers and responds");

        // Simulate ACCEPTED responses.
        PaxosPayload acceptedAfterRecovery = new PaxosPayload();
        acceptedAfterRecovery.setProposalNumber(newProposal);
        acceptedAfterRecovery.setProposedValue("newValueAfterRecovery");

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.ACCEPTED, acceptedAfterRecovery));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.ACCEPTED, acceptedAfterRecovery));
        assertEquals("newValueAfterRecovery", leader.getCommittedValue(),
                "Committed value should match the new proposal after recovery");
    }
}
