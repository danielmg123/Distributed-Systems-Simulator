package com.dss.backend.algorithm.consensus.MultiPaxos;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.multi_paxos.MultiPaxos;
import com.dss.backend.algorithm.consensus.paxos.PaxosPayload;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.VirtualNode;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

public class MultiPaxosAdditionalTests {

    private MultiPaxos leader;
    private MessageRouter router;

    @BeforeEach
    public void setUp() {
        router = new MessageRouter();
        leader = new MultiPaxos();
        leader.setLeader(true);
        leader.setTotalNodes(3);
        leader.setMessageRouter(router);
        leader.setScheduler(Executors.newSingleThreadScheduledExecutor());
        // Register dummy VirtualNodes.
        router.registerNode("node1", new DummyVirtualNode("node1"));
        router.registerNode("node2", new DummyVirtualNode("node2"));
        router.registerNode("node3", new DummyVirtualNode("node3"));
    }

    @Test
    public void testProposalFromNonLeader() {
        MultiPaxos nonLeader = new MultiPaxos();
        nonLeader.setLeader(false);
        nonLeader.setTotalNodes(3);
        nonLeader.setMessageRouter(router);

        nonLeader.propose("nonLeaderValue");

        assertFalse(nonLeader.isPreparePhaseCompleted(), "Non-leader should not complete the prepare phase");
        assertEquals(0, nonLeader.getCurrentProposalNumber(), "Non-leader should not increment proposal counter");
    }

    @Test
    public void testConflictingProposals() {
        leader.propose("value1");
        int currentProposal = leader.getCurrentProposalNumber();

        PaxosPayload promise1 = new PaxosPayload();
        promise1.setProposalNumber(currentProposal);
        promise1.setAcceptedId(10);
        promise1.setAcceptedValue("value2");

        PaxosPayload promise2 = new PaxosPayload();
        promise2.setProposalNumber(currentProposal);
        promise2.setAcceptedId(-1);
        promise2.setAcceptedValue(null);

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.PROMISE, promise1));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.PROMISE, promise2));

        assertTrue(leader.isPreparePhaseCompleted(), "Prepare phase should complete after quorum is reached");

        PaxosPayload acceptedPayload = new PaxosPayload();
        acceptedPayload.setProposalNumber(currentProposal);
        acceptedPayload.setProposedValue("value2");

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.ACCEPTED, acceptedPayload));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.ACCEPTED, acceptedPayload));

        assertEquals("value2", leader.getCommittedValue(), "Committed value should match the highest accepted proposal from promises");
    }

    @Test
    public void testRecoveryAfterNodeFailure() {
        DummyVirtualNode failedNode2 = new DummyVirtualNode("node2");
        failedNode2.failNode();
        router.registerNode("node2", failedNode2);

        leader.propose("recoveryTest");
        int currentProposal = leader.getCurrentProposalNumber();

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

        assertTrue(leader.isPreparePhaseCompleted(), "Prepare phase should complete with responses from node1 and node3 even if node2 is down");

        PaxosPayload acceptedPayload = new PaxosPayload();
        acceptedPayload.setProposalNumber(currentProposal);
        acceptedPayload.setProposedValue("recoveryTest");

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.ACCEPTED, acceptedPayload));
        leader.handleMessage(new SimulationMessage("node3", "self", MessageType.ACCEPTED, acceptedPayload));
        assertEquals("recoveryTest", leader.getCommittedValue(), "Committed value should match the proposal even with one node down");

        failedNode2.recoverNode();

        leader.propose("newValueAfterRecovery");
        int newProposal = leader.getCurrentProposalNumber();

        PaxosPayload promiseRecovered = new PaxosPayload();
        promiseRecovered.setProposalNumber(newProposal);
        promiseRecovered.setAcceptedId(-1);
        promiseRecovered.setAcceptedValue(null);

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.PROMISE, promiseRecovered));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.PROMISE, promiseRecovered));
        assertTrue(leader.isPreparePhaseCompleted(), "Prepare phase should complete after node2 recovers and responds");

        PaxosPayload acceptedAfterRecovery = new PaxosPayload();
        acceptedAfterRecovery.setProposalNumber(newProposal);
        acceptedAfterRecovery.setProposedValue("newValueAfterRecovery");

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.ACCEPTED, acceptedAfterRecovery));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.ACCEPTED, acceptedAfterRecovery));
        assertEquals("newValueAfterRecovery", leader.getCommittedValue(), "Committed value should match the new proposal after recovery");
    }

    // DummyVirtualNode that extends VirtualNode for testing.
    static class DummyVirtualNode extends VirtualNode {

        public DummyVirtualNode(String nodeId) {
            super(createDummyNode(nodeId), new DummyConsensusAlgorithm(), new MessageRouter(),
                    Executors.newSingleThreadExecutor(), Executors.newSingleThreadScheduledExecutor());
            this.start();
        }

        private static Node createDummyNode(String nodeId) {
            Node node = new Node();
            node.setId(nodeId);
            node.setStatus(NodeStatus.ACTIVE);
            return node;
        }
    }

    // DummyConsensusAlgorithm for testing.
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
}