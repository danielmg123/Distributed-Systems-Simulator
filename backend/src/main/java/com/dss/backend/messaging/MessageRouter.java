package com.dss.backend.messaging;

import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MessageRouter implements IMessageRouter {

    private final AppLogger appLogger = new DefaultAppLogger(MessageRouter.class);

    // Map of nodeId -> VirtualNode
    private Map<String, VirtualNode> nodeMap = new ConcurrentHashMap<>();

    public void registerNode(String nodeId, VirtualNode node) {
        nodeMap.put(nodeId, node);
    }

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

    public Set<String> getRegisteredNodeIds() {
        return nodeMap.keySet();
    }
}