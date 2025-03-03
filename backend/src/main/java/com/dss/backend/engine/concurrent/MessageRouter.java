package com.dss.backend.engine.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MessageRouter {

    private static final Logger logger = LoggerFactory.getLogger(MessageRouter.class);

    // Map of nodeId -> VirtualNode
    private Map<String, VirtualNode> nodeMap = new ConcurrentHashMap<>();

    public void registerNode(String nodeId, VirtualNode node) {
        nodeMap.put(nodeId, node);
    }

    public void messageSent(SimulationMessage message) {
        VirtualNode targetNode = nodeMap.get(message.getTargetNodeId());
        if (targetNode != null) {
            logger.debug("Routing message from {} to {} with type {}",
                    message.getSourceNodeId(), message.getTargetNodeId(), message.getType());
            targetNode.enqueueMessage(message);
        } else {
            logger.warn("Target node {} not found for message from {}",
                    message.getTargetNodeId(), message.getSourceNodeId());
        }
    }

    public Set<String> getRegisteredNodeIds() {
        return nodeMap.keySet();
    }
}