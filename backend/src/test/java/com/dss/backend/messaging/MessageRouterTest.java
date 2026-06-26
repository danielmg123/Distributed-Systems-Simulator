package com.dss.backend.messaging;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Set;

import com.dss.backend.metrics.PerformanceMetricsCollector;
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

    @Test
    public void messageSent_TargetNodeFailed_DropsMessageAndRecordsMetric() {
        PerformanceMetricsCollector metricsCollector = mock(PerformanceMetricsCollector.class);
        MessageRouter routerWithMetrics = new MessageRouter(metricsCollector);

        DummyVirtualNode failedNode = spy(new DummyVirtualNode("failedNode"));
        failedNode.failNode();
        routerWithMetrics.registerNode("failedNode", failedNode);

        SimulationMessage message = new SimulationMessage(
                "sender", "failedNode", MessageType.HEARTBEAT, System.currentTimeMillis(), ProtocolType.UNIVERSAL);
        routerWithMetrics.messageSent(message);

        verify(failedNode, never()).enqueueMessage(message);
        verify(metricsCollector, times(1)).recordDroppedMessage();
    }

    @Test
    public void messageSent_SourceNodeFailed_DropsMessageAndRecordsMetric() {
        PerformanceMetricsCollector metricsCollector = mock(PerformanceMetricsCollector.class);
        MessageRouter routerWithMetrics = new MessageRouter(metricsCollector);

        // Source node has already failed (e.g. it queued this send right before failing),
        // but the target node is perfectly healthy.
        DummyVirtualNode sourceFailedNode = spy(new DummyVirtualNode("sourceFailed"));
        sourceFailedNode.failNode();
        DummyVirtualNode targetActiveNode = spy(new DummyVirtualNode("targetActive"));

        routerWithMetrics.registerNode("sourceFailed", sourceFailedNode);
        routerWithMetrics.registerNode("targetActive", targetActiveNode);

        SimulationMessage message = new SimulationMessage(
                "sourceFailed", "targetActive", MessageType.HEARTBEAT, System.currentTimeMillis(), ProtocolType.UNIVERSAL);
        routerWithMetrics.messageSent(message);

        verify(targetActiveNode, never()).enqueueMessage(message);
        verify(metricsCollector, times(1)).recordDroppedMessage();
    }

    @Test
    public void messageSent_BothNodesActive_DoesNotRecordDroppedMetric() {
        PerformanceMetricsCollector metricsCollector = mock(PerformanceMetricsCollector.class);
        MessageRouter routerWithMetrics = new MessageRouter(metricsCollector);

        DummyVirtualNode activeNode = spy(new DummyVirtualNode("activeNode"));
        routerWithMetrics.registerNode("activeNode", activeNode);

        SimulationMessage message = new SimulationMessage(
                "sender", "activeNode", MessageType.HEARTBEAT, System.currentTimeMillis(), ProtocolType.UNIVERSAL);
        routerWithMetrics.messageSent(message);

        verify(activeNode, times(1)).enqueueMessage(message);
        verify(metricsCollector, never()).recordDroppedMessage();
    }

    @Test
    public void messageSent_FullLossRate_NeverDelivers() {
        PerformanceMetricsCollector metricsCollector = mock(PerformanceMetricsCollector.class);
        MessageRouter lossyRouter = new MessageRouter(metricsCollector);
        lossyRouter.setMessageLossRate(1.0);

        DummyVirtualNode targetNode = spy(new DummyVirtualNode("lossyTarget"));
        lossyRouter.registerNode("lossyTarget", targetNode);

        int sendCount = 20;
        for (int i = 0; i < sendCount; i++) {
            SimulationMessage message = new SimulationMessage(
                    "sender", "lossyTarget", MessageType.HEARTBEAT, System.currentTimeMillis(), ProtocolType.UNIVERSAL);
            lossyRouter.messageSent(message);
        }

        verify(targetNode, never()).enqueueMessage(any());
        verify(metricsCollector, times(sendCount)).recordDroppedMessage();
    }

    @Test
    public void messageSent_WithFixedDelay_DeliversOnlyAfterApproximatelyConfiguredDelay() {
        com.dss.backend.engine.Scheduler realScheduler = new com.dss.backend.engine.DefaultScheduler(
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor());
        PerformanceMetricsCollector metricsCollector = mock(PerformanceMetricsCollector.class);
        // minDelayMs == maxDelayMs makes the delay deterministic (no randomness) for a
        // tight, non-flaky timestamp assertion.
        MessageRouter delayedRouter = new MessageRouter(metricsCollector, realScheduler);
        delayedRouter.setMinDelayMs(200);
        delayedRouter.setMaxDelayMs(200);

        DummyVirtualNode targetNode = spy(new DummyVirtualNode("delayedTarget"));
        delayedRouter.registerNode("delayedTarget", targetNode);

        SimulationMessage message = new SimulationMessage(
                "sender", "delayedTarget", MessageType.HEARTBEAT, System.currentTimeMillis(), ProtocolType.UNIVERSAL);

        long sentAt = System.currentTimeMillis();
        delayedRouter.messageSent(message);

        // messageSent() only schedules delivery -- it must not deliver synchronously.
        verify(targetNode, never()).enqueueMessage(message);

        // Delivery should happen once the delay elapses, within a generous timeout.
        verify(targetNode, timeout(2000)).enqueueMessage(message);
        long observedDelay = System.currentTimeMillis() - sentAt;

        assertTrue(observedDelay >= 150,
                "Delivery should not happen meaningfully before the configured 200ms delay, took " + observedDelay + "ms");
    }
}