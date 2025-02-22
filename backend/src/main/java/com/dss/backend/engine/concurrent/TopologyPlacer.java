package com.dss.backend.engine.concurrent;

import com.dss.backend.model.Node;
import com.dss.backend.model.TopologyType;

import java.util.*;

/**
 * Given a topology type and a list of nodes, computes a mapping from each node’s ID
 * to the list of neighbor node IDs.
 */
public class TopologyPlacer {

    public static Map<String, List<String>> assignNeighbors(TopologyType topologyType, List<Node> nodes) {
        Map<String, List<String>> neighborMap = new HashMap<>();
        int n = nodes.size();

        switch (topologyType) {
            case RING:
                // For a ring: each node connects to the next; the last connects to the first.
                for (int i = 0; i < n; i++) {
                    String currentId = nodes.get(i).getId();
                    String nextId = nodes.get((i + 1) % n).getId();
                    neighborMap.put(currentId, Collections.singletonList(nextId));
                }
                break;
            case MESH:
                // Full mesh: every node is connected to every other node.
                for (Node node : nodes) {
                    List<String> neighbors = new ArrayList<>();
                    for (Node other : nodes) {
                        if (!other.getId().equals(node.getId())) {
                            neighbors.add(other.getId());
                        }
                    }
                    neighborMap.put(node.getId(), neighbors);
                }
                break;
            case STAR:
                // In a star, choose one node as the hub (e.g. the first node).
                if (!nodes.isEmpty()) {
                    String hubId = nodes.get(0).getId();
                    List<String> hubNeighbors = new ArrayList<>();
                    for (int i = 1; i < n; i++) {
                        hubNeighbors.add(nodes.get(i).getId());
                    }
                    neighborMap.put(hubId, hubNeighbors);
                    // All other nodes have the hub as their only neighbor.
                    for (int i = 1; i < n; i++) {
                        neighborMap.put(nodes.get(i).getId(), Collections.singletonList(hubId));
                    }
                }
                break;
            case TREE:
                // A simple binary tree mapping.
                for (int i = 0; i < n; i++) {
                    String parentId = nodes.get(i).getId();
                    List<String> children = new ArrayList<>();
                    int left = 2 * i + 1;
                    int right = 2 * i + 2;
                    if (left < n) {
                        children.add(nodes.get(left).getId());
                    }
                    if (right < n) {
                        children.add(nodes.get(right).getId());
                    }
                    neighborMap.put(parentId, children);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported topology type: " + topologyType);
        }
        return neighborMap;
    }
}