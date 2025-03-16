package com.dss.backend.messaging;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <strong>SimulationMessage</strong> represents a single piece of communication
 * exchanged between simulated nodes. Messages carry:
 * <ul>
 *   <li><strong>sourceNodeId</strong>: The node sending the message.</li>
 *   <li><strong>targetNodeId</strong>: The node intended to receive the message.</li>
 *   <li><strong>type</strong>: A {@link MessageType} indicating the purpose or stage of the message
 *       (e.g. PREPARE_REQUEST, HEARTBEAT, ACCEPTED, etc.).</li>
 *   <li><strong>payload</strong>: An arbitrary object (often a consensus-specific payload)
 *       containing additional data for the message.</li>
 *   <li><strong>protocol</strong>: Identifies the consensus or communication protocol
 *       (e.g. PAXOS, RAFT, MULTI_PAXOS) that generated/uses the message.</li>
 * </ul>
 * <p>
 * Nodes never directly call each other; instead, a node sends a <em>SimulationMessage</em>
 * to the central {@link MessageRouter}, which handles delivering it to the target node.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SimulationMessage {
    private String sourceNodeId;
    private String targetNodeId;
    private MessageType type;
    private Object payload;
    private ProtocolType protocol;
}