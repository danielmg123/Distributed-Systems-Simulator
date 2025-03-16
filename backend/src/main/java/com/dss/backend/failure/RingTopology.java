package com.dss.backend.failure;

import com.dss.backend.model.Node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <strong>RingTopology</strong> manages a list of nodes arranged in a ring,
 * where each node monitors its immediate successor’s heartbeat to detect failures.
 * <p>
 * <strong>Usage:</strong>
 * <ul>
 *   <li>Construct with a list of nodes in ring order.</li>
 *   <li>Call {@link #receiveHeartbeat(String)} whenever a node’s heartbeat is received
 *       to update the last-seen time for that node.</li>
 *   <li>Each node periodically calls {@link #checkSuccessorFailure(String)} to see if
 *       its successor is overdue (has not sent a heartbeat recently enough).</li>
 * </ul>
 */
public class RingTopology {

    /**
     * The list of nodes in ring order. The node at index <code>i</code>
     * has a successor at index <code>(i + 1) % ringNodes.size()</code>.
     */
    private final List<Node> ringNodes;

    /**
     * Records the timestamp (in milliseconds) of the last heartbeat from each node.
     * Keyed by node ID.
     */
    private final Map<String, Long> lastHeartbeatMap = new HashMap<>();

    /**
     * If the difference between the current time and the last heartbeat time
     * exceeds this threshold, the node is considered failed.
     */
    private final long failureThresholdMillis = 3000;

    /**
     * Constructs a ring topology with the provided nodes in ring order.
     *
     * @param nodes the list of nodes, assumed to be in ring sequence.
     */
    public RingTopology(List<Node> nodes) {
        this.ringNodes = nodes;
        long now = System.currentTimeMillis();
        // Initialize heartbeat timestamps.
        for (Node node : nodes) {
            lastHeartbeatMap.put(node.getId(), now);
        }
    }

    /**
     * @return the underlying list of nodes, in ring order.
     */
    public List<Node> getNodes() {
        return ringNodes;
    }

    /**
     * Called when a heartbeat is received from a given node.
     * Updates that node’s last heartbeat timestamp to the current system time.
     *
     * @param nodeId the node that sent the heartbeat.
     */
    public void receiveHeartbeat(String nodeId) {
        lastHeartbeatMap.put(nodeId, System.currentTimeMillis());
    }

    /**
     * Determines whether the successor of the given node has failed
     * by checking the time since that successor’s last heartbeat.
     * <p>
     * <strong>Logic:</strong>
     * <ol>
     *   <li>Find the node’s index in the ring.</li>
     *   <li>Compute successor index: <code>(index + 1) % ringNodes.size()</code>.</li>
     *   <li>Check the successor’s last heartbeat time.
     *       If the difference from now exceeds {@link #failureThresholdMillis},
     *       we consider it failed.</li>
     * </ol>
     *
     * @param nodeId the node performing the check (its successor is monitored).
     * @return the successor’s node ID if it appears failed, or <code>null</code> if it is alive.
     */
    public String checkSuccessorFailure(String nodeId) {
        int index = -1;
        for (int i = 0; i < ringNodes.size(); i++) {
            if (ringNodes.get(i).getId().equals(nodeId)) {
                index = i;
                break;
            }
        }
        if (index == -1) {
            // Node not found in the ring
            return null;
        }

        // Compute successor using ring wraparound
        int successorIndex = (index + 1) % ringNodes.size();
        String successorId = ringNodes.get(successorIndex).getId();

        // If no heartbeat entry for the successor, treat as failed
        if (!lastHeartbeatMap.containsKey(successorId)) {
            return successorId;
        }

        long lastHeartbeat = lastHeartbeatMap.get(successorId);
        long now = System.currentTimeMillis();
        // Compare time since last heartbeat to threshold
        if (now - lastHeartbeat > failureThresholdMillis) {
            return successorId; // success is declared "failed"
        }
        return null; // successor is alive
    }
}