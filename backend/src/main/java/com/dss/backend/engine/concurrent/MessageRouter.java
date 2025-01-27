package com.dss.backend.engine.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageRouter {
    
    // nodeId -> VirtualNodeThread
    private Map<String, VirtualNodeThread> nodeThreadMap = new ConcurrentHashMap<>();

    public void registerNode(String nodeId, VirtualNodeThread thread){
        nodeThreadMap.put(nodeId, thread);
    }

    public void messageSent(SimulationMessage message){
        // route to the correct VirtualNode
        VirtualNodeThread targetThread = nodeThreadMap.get(message.getTargetNodeId());

        if(targetThread != null){
            targetThread.enqueueMessage(message);
        }
    }
}