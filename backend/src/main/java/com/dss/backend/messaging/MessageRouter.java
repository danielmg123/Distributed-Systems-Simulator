package com.dss.backend.messaging;

import com.dss.backend.engine.Scheduler;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import com.dss.backend.metrics.DefaultMetricsCollector;
import com.dss.backend.metrics.PerformanceMetricsCollector;
import com.dss.backend.model.NodeStatus;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The <strong>MessageRouter</strong> is responsible for delivering
 * {@link SimulationMessage} objects from a source node to a target node.
 * <p>
 * Each node is registered with the router via {@link #registerNode(String, VirtualNode)},
 * enabling the router to know how to deliver messages to that node.
 * When {@link #messageSent(SimulationMessage)} is called, the router looks up
 * the corresponding {@link VirtualNode} by ID and enqueues the message for processing.
 * <p>
 * This central mechanism decouples nodes from directly sending each other
 * messages, allowing for features like logging, intercepting, or simulating
 * network delays in the future if needed.
 */
public class MessageRouter implements IMessageRouter {

    private final AppLogger appLogger = new DefaultAppLogger(MessageRouter.class);

    // Map of nodeId -> VirtualNode
    private Map<String, VirtualNode> nodeMap = new ConcurrentHashMap<>();

    private final PerformanceMetricsCollector metricsCollector;

    // Used to schedule delayed delivery when maxDelayMs > 0. May be null (e.g. for
    // callers that only care about FAILED-node dropping and never configure a delay),
    // in which case delayed delivery silently falls back to synchronous delivery.
    private final Scheduler scheduler;

    private final Random random = new Random();

    // Chance, in [0.0, 1.0], that any given message is dropped before delivery,
    // independent of node status. Defaults to 0 (no random loss).
    private volatile double messageLossRate = 0.0;

    // Delay range (inclusive) applied to delivery when maxDelayMs > 0. Defaults to 0,
    // meaning messages are delivered synchronously as before this feature existed.
    private volatile long minDelayMs = 0;
    private volatile long maxDelayMs = 0;

    /**
     * Constructs a MessageRouter with its own private, unshared metrics collector and
     * no scheduler (so delay simulation is unavailable -- {@link #setMaxDelayMs(long)}
     * will be silently ignored). Dropped-message counts recorded here won't be visible
     * to anything else -- prefer {@link #MessageRouter(PerformanceMetricsCollector, Scheduler)}
     * when the caller already has a collector and scheduler it wants to share.
     */
    public MessageRouter() {
        this(new DefaultMetricsCollector(), null);
    }

    /**
     * Constructs a MessageRouter that records dropped-message counts (and any other
     * future metrics) into the given shared collector, but with no scheduler (so delay
     * simulation is unavailable).
     *
     * @param metricsCollector the collector to record dropped messages into
     */
    public MessageRouter(PerformanceMetricsCollector metricsCollector) {
        this(metricsCollector, null);
    }

    /**
     * Constructs a MessageRouter that records dropped-message counts into the given
     * shared collector and uses the given scheduler to simulate message delay.
     *
     * @param metricsCollector the collector to record dropped messages into
     * @param scheduler        used to schedule delayed delivery when a delay range is
     *                         configured via {@link #setMinDelayMs(long)}/{@link #setMaxDelayMs(long)};
     *                         may be null if delay simulation won't be used
     */
    public MessageRouter(PerformanceMetricsCollector metricsCollector, Scheduler scheduler) {
        this.metricsCollector = metricsCollector;
        this.scheduler = scheduler;
    }

    /**
     * Sets the probability, in [0.0, 1.0], that any given message is dropped before
     * delivery -- independent of whether the source/target node is FAILED. Values
     * outside [0.0, 1.0] are clamped.
     *
     * @param messageLossRate the new loss probability
     */
    public void setMessageLossRate(double messageLossRate) {
        this.messageLossRate = Math.max(0.0, Math.min(1.0, messageLossRate));
    }

    /**
     * Sets the minimum simulated delivery delay, in milliseconds. Has no effect unless
     * {@link #setMaxDelayMs(long)} is also set to a positive value. Negative values are
     * clamped to 0.
     *
     * @param minDelayMs the new minimum delay
     */
    public void setMinDelayMs(long minDelayMs) {
        this.minDelayMs = Math.max(0, minDelayMs);
    }

    /**
     * Sets the maximum simulated delivery delay, in milliseconds. A value of 0 (the
     * default) disables delay simulation entirely, delivering messages synchronously.
     * Negative values are clamped to 0.
     *
     * @param maxDelayMs the new maximum delay
     */
    public void setMaxDelayMs(long maxDelayMs) {
        this.maxDelayMs = Math.max(0, maxDelayMs);
    }

    /**
     * Registers a node with the router so that subsequent messages
     * targeting <code>nodeId</code> can be delivered.
     *
     * @param nodeId the unique identifier of the node
     * @param node   the {@link VirtualNode} instance representing that node
     */
    public void registerNode(String nodeId, VirtualNode node) {
        nodeMap.put(nodeId, node);
    }

    /**
     * Handles the sending of a {@link SimulationMessage}. The router
     * looks up the {@code targetNodeId} from the message, fetches the corresponding
     * {@link VirtualNode}, and forwards (enqueues) the message to it.
     * <p>
     * The message is dropped (and {@link PerformanceMetricsCollector#recordDroppedMessage()}
     * is incremented) rather than delivered if:
     * <ul>
     *   <li>the target node is not registered at all, or</li>
     *   <li>the target node is currently {@link NodeStatus#FAILED}, or</li>
     *   <li>the source node is registered and currently {@link NodeStatus#FAILED}
     *       (a defensive check for the rare race where a node fails mid-send).</li>
     * </ul>
     * Without these checks, a "failed" node would otherwise keep fully participating
     * in the protocol from every other node's point of view, since nothing else in
     * the simulation enforces crash-stop semantics.
     * <p>
     * If the node-status checks pass, the message is then subject to simulated network
     * conditions, in order:
     * <ol>
     *   <li>Random loss: if {@link #setMessageLossRate(double)} is non-zero, the message
     *       may be dropped (and the dropped-message metric incremented) regardless of
     *       node status.</li>
     *   <li>Random delay: if {@link #setMaxDelayMs(long)} is positive and a scheduler was
     *       provided, delivery is scheduled after a random delay in
     *       [{@code minDelayMs}, {@code maxDelayMs}] instead of happening immediately.
     *       Note this does not re-check node status once the delay elapses -- a node that
     *       fails during the delay window will still receive the message.</li>
     *   <li>Otherwise, delivery happens synchronously, as before this feature existed.</li>
     * </ol>
     *
     * @param message the message being routed from a source node to a target node
     */
    public void messageSent(SimulationMessage message) {
        VirtualNode targetNode = nodeMap.get(message.getTargetNodeId());
        if (targetNode == null) {
            appLogger.info("Target node {} not found for message from {}",
                    message.getTargetNodeId(), message.getSourceNodeId());
            return;
        }

        if (targetNode.getNodeStatus() == NodeStatus.FAILED) {
            appLogger.debug("Dropping message from {} to {} (type {}): target node is FAILED",
                    message.getSourceNodeId(), message.getTargetNodeId(), message.getType());
            metricsCollector.recordDroppedMessage();
            return;
        }

        VirtualNode sourceNode = nodeMap.get(message.getSourceNodeId());
        if (sourceNode != null && sourceNode.getNodeStatus() == NodeStatus.FAILED) {
            appLogger.debug("Dropping message from {} to {} (type {}): source node is FAILED",
                    message.getSourceNodeId(), message.getTargetNodeId(), message.getType());
            metricsCollector.recordDroppedMessage();
            return;
        }

        if (messageLossRate > 0.0 && random.nextDouble() < messageLossRate) {
            appLogger.debug("Dropping message from {} to {} (type {}): random message loss (rate={})",
                    message.getSourceNodeId(), message.getTargetNodeId(), message.getType(), messageLossRate);
            metricsCollector.recordDroppedMessage();
            return;
        }

        if (maxDelayMs > 0 && scheduler != null) {
            long effectiveMinDelay = Math.min(minDelayMs, maxDelayMs);
            long delay = effectiveMinDelay + (long) (random.nextDouble() * (maxDelayMs - effectiveMinDelay));
            appLogger.debug("Delaying message from {} to {} (type {}) by {} ms",
                    message.getSourceNodeId(), message.getTargetNodeId(), message.getType(), delay);
            scheduler.schedule(() -> {
                metricsCollector.recordMessage();
                targetNode.enqueueMessage(message);
            }, delay, TimeUnit.MILLISECONDS);
            return;
        }

        appLogger.debug("Routing message from {} to {} with type {}",
                message.getSourceNodeId(), message.getTargetNodeId(), message.getType());
        metricsCollector.recordMessage();
        targetNode.enqueueMessage(message);
    }

    /**
     * Returns the set of all node IDs that are currently registered with this router.
     *
     * @return a Set of node IDs
     */
    public Set<String> getRegisteredNodeIds() {
        return nodeMap.keySet();
    }
}