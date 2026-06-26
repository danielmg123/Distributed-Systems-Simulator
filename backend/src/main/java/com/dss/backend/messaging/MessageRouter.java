package com.dss.backend.messaging;

import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import com.dss.backend.metrics.DefaultMetricsCollector;
import com.dss.backend.metrics.PerformanceMetricsCollector;
import com.dss.backend.model.NodeStatus;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    /**
     * Constructs a MessageRouter with its own private, unshared metrics collector.
     * Dropped-message counts recorded here won't be visible to anything else --
     * prefer {@link #MessageRouter(PerformanceMetricsCollector)} when the caller
     * already has a collector it wants to expose via a dashboard/metrics endpoint.
     */
    public MessageRouter() {
        this(new DefaultMetricsCollector());
    }

    /**
     * Constructs a MessageRouter that records dropped-message counts (and any other
     * future metrics) into the given shared collector.
     *
     * @param metricsCollector the collector to record dropped messages into
     */
    public MessageRouter(PerformanceMetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
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

        appLogger.debug("Routing message from {} to {} with type {}",
                message.getSourceNodeId(), message.getTargetNodeId(), message.getType());
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