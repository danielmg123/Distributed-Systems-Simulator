package com.dss.backend.engine.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MessageRouter {

    private static final Logger logger = LoggerFactory.getLogger(MessageRouter.class);

    // nodeId -> VirtualNodeThread
    private Map<String, VirtualNodeThread> nodeThreadMap = new ConcurrentHashMap<>();

    public void registerNode(String nodeId, VirtualNodeThread thread){
        nodeThreadMap.put(nodeId, thread);
    }

    public void messageSent(SimulationMessage message) {
        // route to the correct VirtualNode
        VirtualNodeThread targetThread = nodeThreadMap.get(message.getTargetNodeId());
        if (targetThread != null) {
            logger.debug("Routing message from {} to {} with type {}",
                    message.getSourceNodeId(), message.getTargetNodeId(), message.getType());
            targetThread.enqueueMessage(message);
        } else {
            logger.warn("Target node {} not found for message from {}",
                    message.getTargetNodeId(), message.getSourceNodeId());
        }
    }

    public Set<String> getRegisteredNodeIds() {return nodeThreadMap.keySet();}
}