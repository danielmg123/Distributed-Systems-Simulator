package com.dss.backend.algorithm.consensus.MultiPaxos;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.multi_paxos.MultiPaxos;
import com.dss.backend.algorithm.consensus.paxos.PaxosPayload;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.SimulationMessage;
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
        // Set the prepare timeout to 10 seconds (10000 ms) for testing purposes.
        leader.setPrepareTimeoutMillis(10000);
        leader.setTotalNodes(3);
        leader.setMessageRouter(router);
        leader.setScheduler(Executors.newSingleThreadScheduledExecutor());

        // Register dummy nodes for testing
        router.registerNode("node1", new DummyVirtualNode("node1"));
        router.registerNode("node2", new DummyVirtualNode("node2"));
        router.registerNode("node3", new DummyVirtualNode("node3"));
    }

    @Test
    public void testConflictingProposals() throws InterruptedException {
        leader.propose("value1");
        int currentProposal = leader.getCurrentProposalNumber();

        // Simulate promise responses from two nodes with conflicting accepted proposals.
        PaxosPayload promise1 = new PaxosPayload();
        promise1.setProposalNumber(currentProposal);
        promise1.setAcceptedId(10);
        promise1.setAcceptedValue("value2");

        PaxosPayload promise2 = new PaxosPayload();
        promise2.setProposalNumber(currentProposal);
        promise2.setAcceptedId(-1);
        promise2.setAcceptedValue(null);

        // Process promise responses
        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.PROMISE, promise1));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.PROMISE, promise2));

        // Wait until preparePhaseCompleted becomes true (or timeout after 5 seconds)
        boolean prepareCompleted = false;
        for (int i = 0; i < 50; i++) {
            if (leader.isPreparePhaseCompleted()) {
                prepareCompleted = true;
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(prepareCompleted, "Prepare phase should complete after quorum is reached");

        // Now simulate ACCEPTED responses
        PaxosPayload acceptedPayload = new PaxosPayload();
        acceptedPayload.setProposalNumber(currentProposal);
        acceptedPayload.setProposedValue("value2");

        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.ACCEPTED, acceptedPayload));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.ACCEPTED, acceptedPayload));

        // Verify that the committed value matches the highest accepted value from the promises.
        assertEquals("value2", leader.getCommittedValue(), "Committed value should match the highest accepted proposal from promises");
    }

    // Dummy implementations for testing

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