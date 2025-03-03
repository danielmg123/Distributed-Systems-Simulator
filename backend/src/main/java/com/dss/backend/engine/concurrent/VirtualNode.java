package com.dss.backend.engine.concurrent;

import java.util.Map;
import java.util.concurrent.*;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.failure.Heartbeat;
import com.dss.backend.algorithm.failure.PhiAccrual;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtualNode {

    private static final Logger logger = LoggerFactory.getLogger(VirtualNode.class);

    private final Node node;
    private final ConsensusAlgorithm algorithm;
    private final MessageRouter router;

    // A blocking queue for inbound messages.
    private final BlockingQueue<SimulationMessage> inboundQueue = new LinkedBlockingQueue<>();

    // Each neighbor has its own phi detector.
    private final Map<String, PhiAccrual> phiDetectors = new ConcurrentHashMap<>();

    // Dependencies are injected rather than created internally.
    private final ExecutorService messageProcessingExecutor;
    private final ScheduledExecutorService scheduler;

    // Handle to the scheduled phi-checker task.
    private ScheduledFuture<?> phiCheckerFuture;
    private final double phiThreshold = 8.0;

    @Getter @Setter
    private Heartbeat heartbeat;

    // Lifecycle flag.
    private volatile boolean running = false;

    // All dependencies are provided via the constructor.
    public VirtualNode(Node node,
                       ConsensusAlgorithm algorithm,
                       MessageRouter router,
                       ExecutorService messageProcessingExecutor,
                       ScheduledExecutorService scheduler) {
        this.node = node;
        this.algorithm = algorithm;
        this.router = router;
        this.messageProcessingExecutor = messageProcessingExecutor;
        this.scheduler = scheduler;
    }

    /**
     * Starts the virtual node: it schedules its message processing loop and the phi-checker.
     */
    public void start() {
        running = true;
        // Submit the main message processing loop to the provided executor.
        messageProcessingExecutor.submit(this::processMessages);
        // Start the phi-checking task on the injected scheduler.
        startPhiChecker();
    }

    /**
     * Stops the virtual node.
     */
    public void stop() {
        running = false;
        stopPhiChecker();
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
            logger.error("Error processing message from {}: {}", msg.getSourceNodeId(), e.getMessage(), e);
        }
    }

    private PhiAccrual getOrCreatePhiDetectorFor(String neighborId) {
        return phiDetectors.computeIfAbsent(neighborId, id -> new PhiAccrual());
    }

    /**
     * Schedules a periodic task that checks phi values for each neighbor.
     */
    private void startPhiChecker() {
        phiCheckerFuture = scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (Map.Entry<String, PhiAccrual> entry : phiDetectors.entrySet()) {
                double phi = entry.getValue().computePhi(now);
                if (phi >= phiThreshold) {
                    logger.info("Node {} suspects neighbor {} has failed (phi={})", node.getId(), entry.getKey(), phi);
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
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
     * Marks the node as failed.
     */
    public void failNode() {
        node.setStatus(NodeStatus.FAILED);
    }

    /**
     * Recovers the node (marks it as active).
     */
    public void recoverNode() {
        node.setStatus(NodeStatus.ACTIVE);
    }

    public String getNodeId() {
        return node.getId();
    }

    public NodeStatus getNodeStatus() {
        return node.getStatus();
    }
}