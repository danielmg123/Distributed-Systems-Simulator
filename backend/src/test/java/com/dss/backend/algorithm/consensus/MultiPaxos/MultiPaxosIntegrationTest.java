package com.dss.backend.algorithm.consensus.MultiPaxos;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.multi_paxos.MultiPaxos;
import com.dss.backend.algorithm.consensus.paxos.PaxosPayload;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.engine.concurrent.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.dss.backend.engine.concurrent.VirtualNodeThread;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;

import static org.junit.jupiter.api.Assertions.*;

public class MultiPaxosIntegrationTest {

    private MultiPaxos leader;
    private MessageRouter router;

    @BeforeEach
    public void setUp() {
        // Create a new MessageRouter instance.
        router = new MessageRouter();

        // Instantiate the leader MultiPaxos instance.
        leader = new MultiPaxos();
        leader.setLeader(true);
        // Assume we have 3 nodes in our simulated cluster.
        leader.setTotalNodes(3);
        leader.setMessageRouter(router);

        // Register dummy node IDs in the router (simulating a 3-node cluster).
        router.registerNode("node1", new DummyVirtualNode("node1"));
        router.registerNode("node2", new DummyVirtualNode("node2"));
        router.registerNode("node3", new DummyVirtualNode("node3"));
    }

    @Test
    public void testFullPrepareAndAcceptFlow() {
        // Leader proposes a value.
        String proposedValue = "testValue";
        leader.propose(proposedValue);

        // Simulate PROMISE responses from two nodes.
        PaxosPayload promisePayload = new PaxosPayload();
        promisePayload.setProposalNumber(leader.getCurrentProposalNumber());
        // (Assume these nodes have no prior accepted proposals.)
        promisePayload.setAcceptedId(-1);
        promisePayload.setAcceptedValue(null);

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.PROMISE, promisePayload));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.PROMISE, promisePayload));

        // At this point the prepare phase should be complete.
        assertTrue(leader.isPreparePhaseCompleted(), "Prepare phase should be completed after quorum is reached.");

        // Now simulate ACCEPTED responses from two nodes to complete the accept phase.
        PaxosPayload acceptedPayload = new PaxosPayload();
        acceptedPayload.setProposalNumber(leader.getCurrentProposalNumber());
        acceptedPayload.setProposedValue(proposedValue);

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.ACCEPTED, acceptedPayload));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.ACCEPTED, acceptedPayload));

        // Finally, check that the leader has committed the value.
        assertEquals(proposedValue, leader.getCommittedValue(), "Committed value should match the proposed value.");
    }

    // DummyConsensusAlgorithm used for creating dummy VirtualNodeThread instances.
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

    // DummyVirtualNode extends VirtualNodeThread so it can be registered with the router.
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
            // No operation for testing purposes.
        }
    }
}
