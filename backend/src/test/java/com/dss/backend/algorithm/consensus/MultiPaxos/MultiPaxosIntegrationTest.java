package com.dss.backend.algorithm.consensus.MultiPaxos;

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

public class MultiPaxosIntegrationTest {

    private MultiPaxos leader;
    private MessageRouter router;

    @BeforeEach
    public void setUp() {
        router = new MessageRouter();
        // Create a leader instance of MultiPaxos.
        leader = new MultiPaxos();
        leader.setLeader(true);
        leader.setTotalNodes(3);
        leader.setMessageRouter(router);
        leader.setScheduler(Executors.newSingleThreadScheduledExecutor());
        // Register three dummy nodes as VirtualNode instances.
        router.registerNode("node1", new DummyVirtualNode("node1"));
        router.registerNode("node2", new DummyVirtualNode("node2"));
        router.registerNode("node3", new DummyVirtualNode("node3"));
    }

    @Test
    public void testFullPrepareAndAcceptFlow() {
        String proposedValue = "testValue";
        leader.propose(proposedValue);

        PaxosPayload promisePayload = new PaxosPayload();
        promisePayload.setProposalNumber(leader.getCurrentProposalNumber());
        promisePayload.setAcceptedId(-1);
        promisePayload.setAcceptedValue(null);

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.PROMISE, promisePayload));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.PROMISE, promisePayload));

        assertTrue(leader.isPreparePhaseCompleted(), "Prepare phase should be completed after quorum is reached.");

        PaxosPayload acceptedPayload = new PaxosPayload();
        acceptedPayload.setProposalNumber(leader.getCurrentProposalNumber());
        acceptedPayload.setProposedValue(proposedValue);

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.ACCEPTED, acceptedPayload));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.ACCEPTED, acceptedPayload));

        assertEquals(proposedValue, leader.getCommittedValue(), "Committed value should match the proposed value.");
    }

    // DummyVirtualNode for testing. It extends VirtualNode and provides minimal behavior.
    static class DummyVirtualNode extends VirtualNode {

        public DummyVirtualNode(String nodeId) {
            super(createDummyNode(nodeId), new DummyConsensusAlgorithm(), new MessageRouter(),
                    Executors.newSingleThreadExecutor(), Executors.newSingleThreadScheduledExecutor());
            // For testing, we can start the VirtualNode immediately.
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
    static class DummyConsensusAlgorithm implements com.dss.backend.algorithm.consensus.ConsensusAlgorithm {
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