package com.dss.backend.messaging;

import com.dss.backend.model.Node;
import com.dss.backend.model.TopologyType;

import java.util.*;

/**
 * The <strong>TopologyPlacer</strong> computes neighbor relationships among a list of nodes
 * based on a specified {@link TopologyType} (RING, MESH, STAR, TREE).
 * <p>
 * The output is a {@code Map<String, List<String>>} where each key is a node ID
 * and each value is a list of IDs representing that node’s neighbors.
 * Different topologies lead to different adjacency rules:
 * <ul>
 *   <li><strong>RING</strong>: Each node has exactly 1 neighbor—the next node in the ring.
 *       The final node’s neighbor loops back to the first.</li>
 *   <li><strong>MESH</strong>: Every node is connected to every other node (fully connected).</li>
 *   <li><strong>STAR</strong>: One node is designated the “hub”, connected to all other nodes.
 *       All other nodes only connect back to that hub.</li>
 *   <li><strong>TREE</strong>: A simple binary-tree structure where node <code>i</code> in the list
 *       has children at indices <code>2*i + 1</code> and <code>2*i + 2</code> if those indices are in range.</li>
 * </ul>
 * <p>
 * This utility ensures that different network topologies can be generated
 * for simulation scenarios without rewriting adjacency logic multiple times.
 */
public class TopologyPlacer {

    /**
     * Assigns neighbors to each node according to the specified {@link TopologyType}.
     *
     * @param topologyType the type of topology to build (RING, MESH, STAR, or TREE)
     * @param nodes        the list of {@link Node} objects (assumes an ordering if needed)
     * @return a map from node ID to a list of neighbor IDs
     * @throws IllegalArgumentException if an unsupported topology is requested
     */
    public static Map<String, List<String>> assignNeighbors(TopologyType topologyType, List<Node> nodes) {
        Map<String, List<String>> neighborMap = new HashMap<>();
        int n = nodes.size();

        switch (topologyType) {
            case RING:
                // For a ring: each node has exactly one neighbor: the next node in the list,
                // with the last node wrapping around to the first.
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
                // In a star, pick one node as the hub (e.g. the first node).
                // That hub node connects to all others; those others only connect back to the hub.
                if (!nodes.isEmpty()) {
                    String hubId = nodes.get(0).getId();
                    List<String> hubNeighbors = new ArrayList<>();
                    for (int i = 1; i < n; i++) {
                        hubNeighbors.add(nodes.get(i).getId());
                    }
                    neighborMap.put(hubId, hubNeighbors);
                    // All other nodes have the hub as their single neighbor.
                    for (int i = 1; i < n; i++) {
                        neighborMap.put(nodes.get(i).getId(), Collections.singletonList(hubId));
                    }
                }
                break;

            case TREE:
                // A basic binary tree:
                // for index i, left child is (2*i+1), right child is (2*i+2), if valid indices.
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