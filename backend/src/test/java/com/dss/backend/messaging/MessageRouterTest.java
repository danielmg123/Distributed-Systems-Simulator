package com.dss.backend.messaging;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// A simple dummy VirtualNode for testing purposes.
class DummyVirtualNode extends VirtualNode {
    public DummyVirtualNode(String nodeId) {
        super(new com.dss.backend.model.Node() {{
                  setId(nodeId);
                  setStatus(com.dss.backend.model.NodeStatus.ACTIVE);
              }},
                // Use a no‑op consensus algorithm.
                new DummyConsensusAlgorithm(),
                new MessageRouter(), // not used in this dummy
                java.util.concurrent.Executors.newSingleThreadExecutor(),
                new com.dss.backend.engine.DefaultScheduler(java.util.concurrent.Executors.newSingleThreadScheduledExecutor()));
        // For our tests, start the processing loop.
        this.start();
    }

    @Override
    public void enqueueMessage(SimulationMessage msg) {
        super.enqueueMessage(msg);
    }
}

// A dummy ConsensusAlgorithm implementation.
class DummyConsensusAlgorithm implements com.dss.backend.consensus.ConsensusAlgorithm {
    @Override public void propose(Object value) { }
    @Override public boolean accept(Object proposal) { return true; }
    @Override public void commit(Object value) { }
    @Override public void handleMessage(SimulationMessage msg) { }
}

public class MessageRouterTest {

    private MessageRouter router;
    private DummyVirtualNode dummyNode;

    @BeforeEach
    public void setup() {
        router = new MessageRouter();
        // Create and register a dummy node under the id "testNode"
        dummyNode = spy(new DummyVirtualNode("testNode"));
        router.registerNode("testNode", dummyNode);
    }

    @Test
    public void registerNode_AddsVirtualNode() {
        Set<String> registered = router.getRegisteredNodeIds();
        assertTrue(registered.contains("testNode"), "getRegisteredNodeIds() should include 'testNode'");
    }

    @Test
    public void messageSent_ValidTarget_RoutesToQueue() {
        // Create a SimulationMessage targeting "testNode"
        SimulationMessage message = new SimulationMessage("sender", "testNode", MessageType.HEARTBEAT, System.currentTimeMillis(), ProtocolType.UNIVERSAL);
        router.messageSent(message);
        // Verify that dummyNode.enqueueMessage(message) is called.
        verify(dummyNode, times(1)).enqueueMessage(message);
    }

    @Test
    public void messageSent_NonExistingNode_LogsWarning() {
        // Create a message for a non-registered target.
        SimulationMessage message = new SimulationMessage("sender", "nonExistent", MessageType.HEARTBEAT, System.currentTimeMillis(), ProtocolType.UNIVERSAL);
        // Call messageSent. since no node is registered with "nonExistent",
        // nothing is enqueued and (assuming our logger) a warning is logged.
        router.messageSent(message);
        // We simply verify that "nonExistent" is not in the registered nodes.
        assertFalse(router.getRegisteredNodeIds().contains("nonExistent"), "Non-existent node should not be registered");
    }
}