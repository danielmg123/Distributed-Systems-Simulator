package com.dss.backend.consensus.MultiPaxos;

import com.dss.backend.consensus.ConsensusAlgorithm;
import com.dss.backend.consensus.multi_paxos.MultiPaxos;
import com.dss.backend.consensus.paxos.PaxosPayload;
import com.dss.backend.config.SimulationProperties;
import com.dss.backend.engine.DefaultScheduler;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.VirtualNode;
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
        // Create a dummy SimulationProperties for testing
        SimulationProperties props = new SimulationProperties();
        props.setMultipaxosPrepareTimeoutMillis(10000);
        props.setMultipaxosQuorum(0); // Use default quorum calculation

        leader = new MultiPaxos(router, props, new DefaultScheduler(Executors.newSingleThreadScheduledExecutor()));
        leader.setLeader(true);
        leader.setTotalNodes(3);
        // Register three dummy nodes as VirtualNode instances.
        router.registerNode("node1", new DummyVirtualNode("node1"));
        router.registerNode("node2", new DummyVirtualNode("node2"));
        router.registerNode("node3", new DummyVirtualNode("node3"));
    }

    @Test
    public void testFullPrepareAndAcceptFlow() throws InterruptedException {
        String proposedValue = "testValue";
        leader.propose(proposedValue);

        PaxosPayload promisePayload = new PaxosPayload();
        promisePayload.setProposalNumber(leader.getCurrentProposalNumber());
        promisePayload.setAcceptedId(-1);
        promisePayload.setAcceptedValue(null);

        // Simulate receiving PROMISE messages from node1 and node2.
        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.PROMISE, promisePayload));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.PROMISE, promisePayload));

        // Wait until the prepare phase is completed
        boolean prepareCompleted = false;
        for (int i = 0; i < 50; i++) {
            if (leader.isPreparePhaseCompleted()) {
                prepareCompleted = true;
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(prepareCompleted, "Prepare phase should be completed after quorum is reached.");

        PaxosPayload acceptedPayload = new PaxosPayload();
        acceptedPayload.setProposalNumber(leader.getCurrentProposalNumber());
        acceptedPayload.setProposedValue(proposedValue);

        // Simulate receiving ACCEPTED messages.
        leader.handleMessage(new SimulationMessage("node1", "self", MessageType.ACCEPTED, acceptedPayload));
        leader.handleMessage(new SimulationMessage("node2", "self", MessageType.ACCEPTED, acceptedPayload));

        assertEquals(proposedValue, leader.getCommittedValue(), "Committed value should match the proposed value.");
    }

    // Dummy implementations for testing

    static class DummyVirtualNode extends VirtualNode {
        public DummyVirtualNode(String nodeId) {
            super(createDummyNode(nodeId), new DummyConsensusAlgorithm(), new MessageRouter(),
                    Executors.newSingleThreadExecutor(), new DefaultScheduler(Executors.newSingleThreadScheduledExecutor()));
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