package com.dss.backend.messaging;

import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;

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
     * If the target node does not exist, the method logs a warning and the message is dropped.
     *
     * @param message the message being routed from a source node to a target node
     */
    public void messageSent(SimulationMessage message) {
        VirtualNode targetNode = nodeMap.get(message.getTargetNodeId());
        if (targetNode != null) {
            appLogger.debug("Routing message from {} to {} with type {}",
                    message.getSourceNodeId(), message.getTargetNodeId(), message.getType());
            targetNode.enqueueMessage(message);
        } else {
            appLogger.info("Target node {} not found for message from {}",
                    message.getTargetNodeId(), message.getSourceNodeId());
        }
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