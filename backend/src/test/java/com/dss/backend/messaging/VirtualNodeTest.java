package com.dss.backend.messaging;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.*;

import com.dss.backend.consensus.ConsensusAlgorithm;
import com.dss.backend.engine.DefaultScheduler;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.failure.Heartbeat;
import com.dss.backend.failure.PhiAccrual;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * We create test subclasses that either prevent automatic processing or expose helper methods.
 */
public class VirtualNodeTest {

    // A test subclass that does not automatically start processing messages.
    static class NonProcessingVirtualNode extends VirtualNode {
        public NonProcessingVirtualNode(Node node, ConsensusAlgorithm algorithm, MessageRouter router, Scheduler scheduler) {
            super(node, algorithm, router, scheduler);
            // Override start() so we do not kick off the asynchronous loop.
        }
        // Expose the inbound queue size via reflection.
        public int getInboundQueueSize() {
            try {
                java.lang.reflect.Field f = VirtualNode.class.getDeclaredField("inboundQueue");
                f.setAccessible(true);
                @SuppressWarnings("unchecked")
                java.util.concurrent.BlockingQueue<SimulationMessage> queue = (java.util.concurrent.BlockingQueue<SimulationMessage>) f.get(this);
                return queue.size();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        // Expose a method to process one message from the queue synchronously.
        public void processNextMessage() {
            try {
                java.lang.reflect.Field f = VirtualNode.class.getDeclaredField("inboundQueue");
                f.setAccessible(true);
                @SuppressWarnings("unchecked")
                java.util.concurrent.BlockingQueue<SimulationMessage> queue = (java.util.concurrent.BlockingQueue<SimulationMessage>) f.get(this);
                SimulationMessage msg = queue.poll();
                if (msg != null) {
                    // Directly call processMessage
                    java.lang.reflect.Method m = VirtualNode.class.getDeclaredMethod("processMessage", SimulationMessage.class);
                    m.setAccessible(true);
                    m.invoke(this, msg);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    // A dummy consensus algorithm that we spy on.
    static class DummyConsensusAlgorithmSpy implements ConsensusAlgorithm {
        @Override public void propose(Object value) { }
        @Override public boolean accept(Object proposal) { return true; }
        @Override public void commit(Object value) { }
        @Override public void handleMessage(SimulationMessage msg) { }
    }

    // A test subclass that exposes a helper for running the phi check using a spy logger.
    static class TestVirtualNodeWithPhiSpy extends VirtualNode {
        private final AppLogger testLogger;
        public TestVirtualNodeWithPhiSpy(Node node, ConsensusAlgorithm algorithm, MessageRouter router, Scheduler scheduler, AppLogger testLogger) {
            super(node, algorithm, router, scheduler);
            this.testLogger = testLogger;
            // Do not start the automatic phi checker.
        }
        public void runPhiCheckNow() {
            long now = System.currentTimeMillis();
            try {
                java.lang.reflect.Field f = VirtualNode.class.getDeclaredField("phiDetectors");
                f.setAccessible(true);
                @SuppressWarnings("unchecked")
                Map<String, PhiAccrual> detectors = (Map<String, PhiAccrual>) f.get(this);
                for (Map.Entry<String, PhiAccrual> entry : detectors.entrySet()) {
                    double phi = entry.getValue().computePhi(now);
                    if (phi >= 8.0) {
                        testLogger.info("Node {} suspects neighbor {} has failed (phi={})", getNodeId(), entry.getKey(), phi);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private NonProcessingVirtualNode testNode;
    private DummyConsensusAlgorithmSpy consensusSpy;
    private MessageRouter router;
    private Scheduler scheduler;

    @BeforeEach
    public void setup() {
        router = new MessageRouter();
        scheduler = new DefaultScheduler(Executors.newSingleThreadScheduledExecutor());
        consensusSpy = spy(new DummyConsensusAlgorithmSpy());
        Node node = new Node();
        node.setId("vn1");
        node.setStatus(NodeStatus.ACTIVE);
        testNode = new NonProcessingVirtualNode(node, consensusSpy, router, scheduler);
        // Note: We do NOT call start() so that messages remain in the inbound queue.
    }

    @Test
    public void enqueueMessage_PlacesMessageIntoInboundQueue() {
        SimulationMessage message = new SimulationMessage("sender", "vn1", MessageType.HEARTBEAT, System.currentTimeMillis(), ProtocolType.UNIVERSAL);
        testNode.enqueueMessage(message);
        // Since we haven't started processing, the message should remain in the queue.
        assertEquals(1, testNode.getInboundQueueSize(), "Inbound queue should contain one message after enqueueMessage()");
    }

    @Test
    public void processMessages_DelegatesToConsensusAlgorithmForNonHeartbeat() {
        // Enqueue a non-heartbeat message (e.g. PROPOSAL type).
        SimulationMessage message = new SimulationMessage("sender", "vn1", MessageType.PROPOSAL, "testProposal", ProtocolType.UNIVERSAL);
        testNode.enqueueMessage(message);
        // Process one message synchronously.
        testNode.processNextMessage();
        // Verify that consensusSpy.handleMessage(message) was called.
        verify(consensusSpy, times(1)).handleMessage(message);
    }

    @Test
    public void propose_DelegatesToConsensusAlgorithm() {
        testNode.propose("set x=1");
        verify(consensusSpy, times(1)).propose("set x=1");
    }

    @Test
    public void getRoleLabel_DelegatesToConsensusAlgorithm() {
        // DummyConsensusAlgorithmSpy doesn't override getRoleLabel(), so it falls back
        // to ConsensusAlgorithm's default (null) -- same as every non-Raft protocol here.
        assertNull(testNode.getRoleLabel());
        verify(consensusSpy, times(1)).getRoleLabel();
    }

    @Test
    public void phiCheckerTask_SuspectsFailureWhenPhiAboveThreshold() {
        // Create a TestVirtualNodeWithPhiSpy using a spy logger.
        AppLogger spyLogger = spy(new DefaultAppLogger(TestVirtualNodeWithPhiSpy.class));
        Node node = new Node();
        node.setId("vn1");
        node.setStatus(NodeStatus.ACTIVE);
        TestVirtualNodeWithPhiSpy nodeWithPhiSpy =
                new TestVirtualNodeWithPhiSpy(node, consensusSpy, router, scheduler, spyLogger);
        // Insert an entry into the phiDetectors map with two heartbeat records.
        try {
            java.lang.reflect.Field f = VirtualNode.class.getDeclaredField("phiDetectors");
            f.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, PhiAccrual> detectors = (Map<String, PhiAccrual>) f.get(nodeWithPhiSpy);
            PhiAccrual detector = new PhiAccrual();
            long baseTime = System.currentTimeMillis() - 10000; // 10 seconds ago
            // Record two heartbeats so that an interval is recorded.
            detector.recordHeartbeat(baseTime);
            detector.recordHeartbeat(baseTime + 500); // interval = 500 ms
            detectors.put("neighbor1", detector);
        } catch (Exception e) {
            fail("Reflection failed: " + e.getMessage());
        }
        // Run the phi check helper immediately.
        nodeWithPhiSpy.runPhiCheckNow();
        // Verify that our spy logger logged an info message containing "suspects neighbor".
        verify(spyLogger, atLeastOnce()).info(contains("suspects neighbor"), any(), any(), any());
    }

    @Test
    public void failNode_StopsMessageProcessingAndHeartbeat_RecoverNode_ResumesBoth() throws Exception {
        Node node = new Node();
        node.setId("vn-fail-test");
        node.setStatus(NodeStatus.ACTIVE);

        ConsensusAlgorithm spyAlgorithm = spy(new DummyConsensusAlgorithmSpy());
        Heartbeat mockHeartbeat = mock(Heartbeat.class);

        // A real, fully-started VirtualNode (not the NonProcessingVirtualNode test subclass),
        // since we need its actual processing loop running so we can prove failNode()
        // really stops it rather than just flipping a status flag.
        VirtualNode realNode = new VirtualNode(node, spyAlgorithm, router, scheduler);
        realNode.setHeartbeat(mockHeartbeat);
        realNode.start();

        try {
            // Sanity check: the node actually processes messages while ACTIVE.
            SimulationMessage beforeFail = new SimulationMessage(
                    "sender", "vn-fail-test", MessageType.PROPOSAL, "beforeFail", ProtocolType.UNIVERSAL);
            realNode.enqueueMessage(beforeFail);
            verify(spyAlgorithm, timeout(2000)).handleMessage(beforeFail);

            realNode.failNode();

            assertEquals(NodeStatus.FAILED, realNode.getNodeStatus(),
                    "failNode() should mark the node FAILED");
            verify(mockHeartbeat, times(1)).stop();

            // A message enqueued after failure must never be processed -- the loop is
            // actually stopped, not just the status flag.
            SimulationMessage afterFail = new SimulationMessage(
                    "sender", "vn-fail-test", MessageType.PROPOSAL, "afterFail", ProtocolType.UNIVERSAL);
            realNode.enqueueMessage(afterFail);
            Thread.sleep(300);
            verify(spyAlgorithm, never()).handleMessage(afterFail);

            // Calling failNode() again on an already-failed node must be a no-op.
            realNode.failNode();
            verify(mockHeartbeat, times(1)).stop();

            realNode.recoverNode();

            assertEquals(NodeStatus.ACTIVE, realNode.getNodeStatus(),
                    "recoverNode() should mark the node ACTIVE again");
            verify(mockHeartbeat, times(1)).start(scheduler);

            // Processing must actually resume after recovery.
            SimulationMessage afterRecover = new SimulationMessage(
                    "sender", "vn-fail-test", MessageType.PROPOSAL, "afterRecover", ProtocolType.UNIVERSAL);
            realNode.enqueueMessage(afterRecover);
            verify(spyAlgorithm, timeout(2000)).handleMessage(afterRecover);

            // Calling recoverNode() again on an already-active node must be a no-op.
            realNode.recoverNode();
            verify(mockHeartbeat, times(1)).start(scheduler);
        } finally {
            realNode.stop();
        }
    }
}