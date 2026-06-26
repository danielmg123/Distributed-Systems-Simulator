package com.dss.backend.performance;

import com.dss.backend.config.SimulationProperties;
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
import com.dss.backend.consensus.raft.LogEntry;
import com.dss.backend.consensus.raft.Raft;
import com.dss.backend.consensus.raft.RaftPayload;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
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

    /**
     * Regression test for the VirtualNode concurrency bug fixed in Phase 2.4: before the
     * fix, processMessages() took a message off the queue and resubmitted *processing*
     * of it as a second, independent task onto a shared multi-thread executor, instead
     * of processing it inline before looking at the next message. That let the dispatch
     * loop dequeue and resubmit entry N+1's message before entry N's append had actually
     * finished mutating Raft's unsynchronized {@code log} (a plain ArrayList) -- so
     * entry N+1's {@code prevLogIndex} check could run against a log that doesn't yet
     * contain entry N, spuriously rejecting it, or two appends could race on the same
     * ArrayList directly. Each VirtualNode now owns a dedicated single-thread executor
     * and processes messages inline, so message N+1 can never even begin until message
     * N's full effect on the log is visible.
     * <p>
     * Five separate APPEND_ENTRIES messages (one new entry each, correctly chained via
     * prevLogIndex/prevLogTerm) are handed off between 5 threads -- thread i sends only
     * after thread i-1's send has returned -- so messages always *enqueue* in logical
     * order 0..4. That isolates the bug under test to the dispatch/processing race
     * described above rather than to enqueue ordering, while still exercising genuine
     * thread concurrency between the senders and the follower's own processing thread.
     * Repeated 50x because a timing-dependent bug like this can easily pass once by luck.
     */
    @Test
    public void concurrentAppendEntries_ProcessedSerially_LogEndsUpExactlyCorrect() throws Exception {
        int entryCount = 5;
        for (int run = 0; run < 50; run++) {
            ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            Scheduler scheduler = new DefaultScheduler(scheduledExecutor);
            MessageRouter router = new MessageRouter();
            // A long election timeout keeps this test isolated from Raft's own
            // (otherwise real, randomized) election timer -- this test is about message
            // processing order, not elections.
            SimulationProperties simulationProperties = new SimulationProperties();
            simulationProperties.setRaftElectionTimeoutMinMillis(60_000);
            simulationProperties.setRaftElectionTimeoutMaxMillis(120_000);
            Raft follower = new Raft("follower", java.util.Arrays.asList("leader", "follower"), router,
                    scheduler, simulationProperties);
            VirtualNode followerVNode = new VirtualNode(createTestNode("follower"), follower, router, scheduler);
            router.registerNode("follower", followerVNode);
            followerVNode.start();

            ExecutorService senderPool = Executors.newFixedThreadPool(entryCount);
            // gates[i] releases the thread sending entry i; gates[entryCount] is unused.
            CountDownLatch[] gates = new CountDownLatch[entryCount + 1];
            for (int i = 0; i <= entryCount; i++) {
                gates[i] = new CountDownLatch(1);
            }
            CountDownLatch doneLatch = new CountDownLatch(entryCount);

            try {
                for (int i = 0; i < entryCount; i++) {
                    final int index = i;
                    senderPool.submit(() -> {
                        try {
                            gates[index].await();
                            RaftPayload payload = new RaftPayload();
                            payload.setType(MessageType.APPEND_ENTRIES);
                            payload.setTerm(1);
                            payload.setLeaderId("leader");
                            payload.setPrevLogIndex(index - 1);
                            payload.setPrevLogTerm(index == 0 ? -1 : 1);
                            payload.setEntries(List.of(new LogEntry(1, "cmd" + index)));
                            payload.setLeaderCommit(entryCount - 1);

                            SimulationMessage msg = SimulationMessageFactory.createMessage(
                                    "leader", "follower", MessageType.APPEND_ENTRIES, payload, ProtocolType.RAFT);
                            router.messageSent(msg);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            gates[index + 1].countDown();
                            doneLatch.countDown();
                        }
                    });
                }
                gates[0].countDown();
                assertTrue(doneLatch.await(5, TimeUnit.SECONDS),
                        "Senders did not finish in time on run " + run);

                // Poll with a bounded timeout, since the follower's single processing
                // thread drains the queue asynchronously.
                List<LogEntry> finalLog = null;
                for (int i = 0; i < 100; i++) {
                    finalLog = follower.getLog();
                    if (finalLog.size() >= entryCount) {
                        break;
                    }
                    Thread.sleep(10);
                }

                assertNotNull(finalLog, "run " + run);
                assertEquals(entryCount, finalLog.size(),
                        "Log should have exactly " + entryCount + " entries, no duplicates/gaps, on run " + run);
                for (int i = 0; i < entryCount; i++) {
                    assertEquals("cmd" + i, finalLog.get(i).getCommand(), "Entry " + i + " mismatch on run " + run);
                }
            } finally {
                senderPool.shutdown();
                followerVNode.stop();
                scheduler.shutdown();
            }
        }
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
                    }, router,
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