package com.dss.backend.messaging;

import com.dss.backend.consensus.ConsensusAlgorithm;
import com.dss.backend.failure.Heartbeat;
import com.dss.backend.failure.PhiAccrual;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import lombok.Getter;
import lombok.Setter;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class VirtualNode {

    private final AppLogger appLogger = new DefaultAppLogger(VirtualNode.class);

    private final Node node;
    private final ConsensusAlgorithm algorithm;
    private final MessageRouter router;

    // A blocking queue for inbound messages.
    private final BlockingQueue<SimulationMessage> inboundQueue = new LinkedBlockingQueue<>();

    // Each neighbor has its own phi detector.
    private final Map<String, PhiAccrual> phiDetectors = new ConcurrentHashMap<>();

    // Dependencies are injected rather than created internally.
    private final ExecutorService messageProcessingExecutor;
    private final Scheduler scheduler;

    // Handle to the scheduled phi-checker task.
    private ScheduledFuture<?> phiCheckerFuture;
    private final double phiThreshold = 8.0;

    @Getter @Setter
    private Heartbeat heartbeat;

    // Lifecycle flag.
    private volatile boolean running = false;

    // Handle to the running processMessages() loop, so stop()/failNode() can interrupt
    // a blocked inboundQueue.take() immediately instead of waiting for the next message
    // to arrive before the loop notices `running` flipped to false.
    private volatile Future<?> processingLoopFuture;

    // All dependencies are provided via the constructor.
    public VirtualNode(Node node,
                       ConsensusAlgorithm algorithm,
                       MessageRouter router,
                       ExecutorService messageProcessingExecutor,
                       Scheduler scheduler) {
        this.node = node;
        this.algorithm = algorithm;
        this.router = router;
        this.messageProcessingExecutor = messageProcessingExecutor;
        this.scheduler = scheduler;
    }

    /**
     * Starts the virtual node: it schedules its message processing loop and the phi-checker.
     * Idempotent -- calling this on an already-running node is a no-op, so failNode()/
     * recoverNode() can call stop()/start() repeatedly without side effects.
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        // Submit the main message processing loop to the provided executor, keeping a
        // handle to it so stop() can interrupt it immediately rather than waiting for
        // the next message.
        processingLoopFuture = messageProcessingExecutor.submit(this::processMessages);
        // Start the phi-checking task on the injected scheduler.
        startPhiChecker();
    }

    /**
     * Stops the virtual node: halts message processing, the phi-checker, and the
     * heartbeat. Idempotent -- calling this on an already-stopped node is a no-op.
     * <p>
     * Cancelling {@link #processingLoopFuture} with {@code mayInterruptIfRunning=true}
     * interrupts a blocked {@code inboundQueue.take()} immediately; without this, the
     * processing loop would only notice {@code running} flipped to false after its next
     * message arrives, which could be never.
     */
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (processingLoopFuture != null) {
            processingLoopFuture.cancel(true);
        }
        stopPhiChecker();
        if (heartbeat != null) {
            heartbeat.stop();
        }
    }

    /**
     * Continuously takes messages from the inbound queue and dispatches them.
     */
    private void processMessages() {
        while (running) {
            try {
                SimulationMessage msg = inboundQueue.take();
                // Delegate processing to the executor.
                messageProcessingExecutor.submit(() -> processMessage(msg));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Processes an individual message.
     */
    private void processMessage(SimulationMessage msg) {
        try {
            // Handle heartbeat messages separately.
            if (msg.getType() == MessageType.HEARTBEAT && msg.getPayload() instanceof Long) {
                long heartbeatTime = (Long) msg.getPayload();
                PhiAccrual detector = getOrCreatePhiDetectorFor(msg.getSourceNodeId());
                detector.recordHeartbeat(heartbeatTime);
            } else {
                // Delegate to the consensus algorithm (or other logic) for non-heartbeat messages.
                algorithm.handleMessage(msg);
            }
        } catch (Exception e) {
            appLogger.error("Error processing message from {}: {}", msg.getSourceNodeId(), e.getMessage(), e);
        }
    }

    private PhiAccrual getOrCreatePhiDetectorFor(String neighborId) {
        return phiDetectors.computeIfAbsent(neighborId, id -> new PhiAccrual());
    }

    /**
     * Schedules a periodic task that checks phi values for each neighbor.
     */
    private void startPhiChecker() {
        try {
            phiCheckerFuture = scheduler.scheduleAtFixedRate(() -> {
                long now = System.currentTimeMillis();
                for (Map.Entry<String, PhiAccrual> entry : phiDetectors.entrySet()) {
                    double phi = entry.getValue().computePhi(now);
                    if (phi >= phiThreshold) {
                        appLogger.info("Node {} suspects neighbor {} has failed (phi={})", node.getId(), entry.getKey(), phi);
                    }
                }
            }, 0, 1, TimeUnit.SECONDS);
        } catch (RejectedExecutionException ex) {
            // Scheduler is shutdown; do nothing.
        }
    }

    private void stopPhiChecker() {
        if (phiCheckerFuture != null) {
            phiCheckerFuture.cancel(true);
        }
    }

    /**
     * Enqueues an incoming message for later processing.
     */
    public void enqueueMessage(SimulationMessage msg) {
        inboundQueue.offer(msg);
    }

    /**
     * Marks the node as failed and actually makes it stop participating: halts message
     * processing (so it neither reacts to nor learns about further protocol traffic),
     * the phi-checker, and the heartbeat. Idempotent -- calling this on an
     * already-failed node is a no-op.
     * <p>
     * Note this only stops the node's own ability to send/process messages. As of this
     * change, {@link MessageRouter#messageSent(SimulationMessage)} still attempts
     * delivery to FAILED nodes the same as any other node -- those messages will now
     * simply sit in this node's inbound queue forever rather than being processed
     * (since the processing loop is stopped). Making the router itself drop messages
     * to/from FAILED nodes is a separate, still-open piece of work.
     */
    public void failNode() {
        if (node.getStatus() == NodeStatus.FAILED) {
            return;
        }
        node.setStatus(NodeStatus.FAILED);
        stop();
    }

    /**
     * Recovers a failed node: marks it active again and resumes message processing,
     * the phi-checker, and the heartbeat. Idempotent -- calling this on a node that
     * isn't currently FAILED is a no-op.
     */
    public void recoverNode() {
        if (node.getStatus() != NodeStatus.FAILED) {
            return;
        }
        node.setStatus(NodeStatus.ACTIVE);
        start();
        if (heartbeat != null) {
            heartbeat.start(scheduler);
        }
    }

    public String getNodeId() {
        return node.getId();
    }

    public NodeStatus getNodeStatus() {
        return node.getStatus();
    }
}