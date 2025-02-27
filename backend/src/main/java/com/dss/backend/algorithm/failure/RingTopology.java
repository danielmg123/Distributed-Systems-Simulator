package com.dss.backend.algorithm.failure;

import com.dss.backend.model.Node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RingTopology {
    private final List<Node> ringNodes;
    // Track the last heartbeat timestamp for each node (by nodeId)
    private final Map<String, Long> lastHeartbeatMap = new HashMap<>();
    // A threshold (in ms) after which a node is considered failed.
    private final long failureThresholdMillis = 3000;

    public RingTopology(List<Node> nodes) {
        // Assume the provided list is already in ring order.
        this.ringNodes = nodes;
        long now = System.currentTimeMillis();
        for (Node node : nodes) {
            lastHeartbeatMap.put(node.getId(), now);
        }
    }

    public List<Node> getNodes() {
        return ringNodes;
    }

    // Call this method when a heartbeat is received from a node.
    public void receiveHeartbeat(String nodeId) {
        lastHeartbeatMap.put(nodeId, System.currentTimeMillis());
    }

    // For a given node, check whether its immediate successor has failed.
    public String checkSuccessorFailure(String nodeId) {
        int index = -1;
        for (int i = 0; i < ringNodes.size(); i++) {
            if (ringNodes.get(i).getId().equals(nodeId)) {
                index = i;
                break;
            }
        }
        if (index == -1) {
            return null; // node not found in ring
        }
        int successorIndex = (index + 1) % ringNodes.size();
        String successorId = ringNodes.get(successorIndex).getId();

        // Ensure the heartbeat entry exists for the successor before checking
        if (!lastHeartbeatMap.containsKey(successorId)) {
            return successorId; // Assume failure if no heartbeat is recorded
        }

        long lastHeartbeat = lastHeartbeatMap.get(successorId);
        if (System.currentTimeMillis() - lastHeartbeat > failureThresholdMillis) {
            return successorId;
        }
        return null; // successor is alive
    }
}
