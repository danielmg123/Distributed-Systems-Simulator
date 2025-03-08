package com.dss.backend.performance;

import com.dss.backend.engine.DefaultScheduler;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.ProtocolType;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.SimulationMessageFactory;
import com.dss.backend.messaging.VirtualNode;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import com.dss.backend.consensus.ConsensusAlgorithm;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class ConcurrencyTests {

    /**
     * Test that multiple threads sending messages concurrently through the MessageRouter
     * do not cause deadlocks and that the target VirtualNode receives all messages.
     */
    @Test
    public void multiThreadedMessageSending_EnsuresNoDeadlocks() throws InterruptedException {
        MessageRouter router = new MessageRouter();
        // Create a test virtual node that counts received messages.
        TestVirtualNode testNode = new TestVirtualNode(createTestNode("testNode"), router);
        router.registerNode("testNode", testNode);

        int threadCount = 100;
        int messagesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        // Each thread sends messages concurrently.
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < messagesPerThread; j++) {
                        SimulationMessage msg = SimulationMessageFactory.createMessage(
                                "sender", "testNode", MessageType.HEARTBEAT, System.currentTimeMillis(), ProtocolType.UNIVERSAL);
                        router.messageSent(msg);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        // Start all threads
        startLatch.countDown();
        // Wait for all threads to finish
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "Message sending threads did not finish in time");

        // Give some time for the virtual node to process enqueued messages.
        Thread.sleep(2000);
        int expectedMessages = threadCount * messagesPerThread;
        assertEquals(expectedMessages, testNode.getReceivedCount(), "All messages should be received");
        executor.shutdown();
    }

    /**
     * Test that multiple threads concurrently enqueuing messages into a VirtualNode
     * results in all messages being processed.
     */
    @Test
    public void concurrentVirtualNodeEnqueue_ProcessesAllMessages() throws InterruptedException {
        MessageRouter router = new MessageRouter();
        TestVirtualNode testNode = new TestVirtualNode(createTestNode("vn1"), router);
        // We do not start the automatic processing loop in TestVirtualNode.
        int threadCount = 50;
        int messagesPerThread = 200;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        // Concurrently enqueue messages.
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < messagesPerThread; j++) {
                        SimulationMessage msg = SimulationMessageFactory.createMessage(
                                "sender", "vn1", MessageType.PROPOSAL, "data", ProtocolType.UNIVERSAL);
                        testNode.enqueueMessage(msg);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "Enqueueing threads did not finish in time");

        int totalMessages = threadCount * messagesPerThread;
        // Process all messages manually.
        while (testNode.getInboundQueueSize() > 0) {
            testNode.processNextMessage();
        }
        // Verify that all messages were processed.
        assertEquals(totalMessages, testNode.getProcessedCount(), "All enqueued messages should be processed");
        executor.shutdown();
    }

    /**
     * Test that concurrently scheduling tasks via the DefaultScheduler works without deadlock
     * and that all tasks are executed.
     */
    @Test
    public void concurrentScheduling_TasksComplete() throws InterruptedException {
        int threadCount = 20;
        int tasksPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(10);
        Scheduler scheduler = new DefaultScheduler(scheduledExecutor);
        CountDownLatch latch = new CountDownLatch(threadCount * tasksPerThread);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                for (int j = 0; j < tasksPerThread; j++) {
                    scheduler.schedule(() -> latch.countDown(), 100, TimeUnit.MILLISECONDS);
                }
            });
        }
        // Wait for all tasks to complete.
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Scheduled tasks did not complete in time");
        executor.shutdown();
        scheduledExecutor.shutdown();
    }

    // Helper method to create a simple Node.
    private Node createTestNode(String id) {
        Node node = new Node();
        node.setId(id);
        node.setStatus(NodeStatus.ACTIVE);
        return node;
    }

    /**
     * A test subclass of VirtualNode that counts received messages and processed messages.
     * For our concurrency tests we override enqueueMessage and expose the inbound queue size.
     */
    private static class TestVirtualNode extends VirtualNode {
        private final AtomicInteger receivedCount = new AtomicInteger(0);
        private final AtomicInteger processedCount = new AtomicInteger(0);

        public TestVirtualNode(Node node, MessageRouter router) {
            // Use an anonymous inner class to provide a dummy ConsensusAlgorithm implementation.
            super(node, new ConsensusAlgorithm() {
                        @Override
                        public void propose(Object value) { }
                        @Override
                        public boolean accept(Object proposal) { return true; }
                        @Override
                        public void commit(Object value) { }
                        @Override
                        public void handleMessage(SimulationMessage msg) { }
                    }, router, Executors.newSingleThreadExecutor(),
                    new DefaultScheduler(Executors.newSingleThreadScheduledExecutor()));
            // Do not start automatic processing to control it manually.
        }

        @Override
        public void enqueueMessage(SimulationMessage msg) {
            receivedCount.incrementAndGet();
            super.enqueueMessage(msg);
        }

        /**
         * Returns the total number of messages enqueued.
         */
        public int getReceivedCount() {
            return receivedCount.get();
        }

        /**
         * Exposes the current inbound queue size.
         */
        public int getInboundQueueSize() {
            try {
                java.lang.reflect.Field f = VirtualNode.class.getDeclaredField("inboundQueue");
                f.setAccessible(true);
                BlockingQueue<SimulationMessage> queue = (BlockingQueue<SimulationMessage>) f.get(this);
                return queue.size();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Manually processes one message from the queue.
         */
        public void processNextMessage() {
            try {
                java.lang.reflect.Field f = VirtualNode.class.getDeclaredField("inboundQueue");
                f.setAccessible(true);
                BlockingQueue<SimulationMessage> queue = (BlockingQueue<SimulationMessage>) f.get(this);
                SimulationMessage msg = queue.poll();
                if (msg != null) {
                    // Instead of calling the real processMessage, we simply count it.
                    processedCount.incrementAndGet();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Returns the total number of messages processed.
         */
        public int getProcessedCount() {
            return processedCount.get();
        }
    }

    /**
     * A test subclass that exposes a helper for running the phi check using a spy logger.
     */
    private static class TestVirtualNodeWithPhiSpy extends VirtualNode {
        private final AppLogger testLogger;
        public TestVirtualNodeWithPhiSpy(Node node, ConsensusAlgorithm algorithm, MessageRouter router, Scheduler scheduler, AppLogger testLogger) {
            super(node, algorithm, router, Executors.newSingleThreadExecutor(), scheduler);
            this.testLogger = testLogger;
            // Do not start the automatic phi checker.
        }
        public void runPhiCheckNow() {
            long now = System.currentTimeMillis();
            try {
                java.lang.reflect.Field f = VirtualNode.class.getDeclaredField("phiDetectors");
                f.setAccessible(true);
                @SuppressWarnings("unchecked")
                Map<String, Object> detectors = (Map<String, Object>) f.get(this);
                for (Map.Entry<String, Object> entry : detectors.entrySet()) {
                    // Using reflection to call computePhi on each detector
                    double phi = (double) entry.getValue().getClass()
                            .getMethod("computePhi", long.class)
                            .invoke(entry.getValue(), now);
                    if (phi >= 8.0) {
                        testLogger.info("Node {} suspects neighbor {} has failed (phi={})", getNodeId(), entry.getKey(), phi);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}