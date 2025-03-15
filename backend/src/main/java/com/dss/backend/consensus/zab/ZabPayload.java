package com.dss.backend.consensus.zab;

import com.dss.backend.messaging.MessageType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Defines the payload structure for Zab messages. Each message includes:
 * <ul>
 *   <li><strong>type</strong>: One of the Zab message types (PROPOSAL, ACK, COMMIT).</li>
 *   <li><strong>zxid</strong>: The unique transaction ID for this proposal.
 *       This strictly increases with every new proposal from the leader.</li>
 *   <li><strong>proposedValue</strong>: The data or command to be replicated
 *       and eventually committed.</li>
 * </ul>
 *
 * <p><strong>Usage:</strong></p>
 * <ul>
 *   <li>A leader includes the <em>zxid</em> and the <em>proposedValue</em> in a
 *       {@link MessageType#PROPOSAL} message to followers.</li>
 *   <li>Followers send an {@link MessageType#ACK} referencing the same zxid
 *       after storing the proposal locally.</li>
 *   <li>Once the leader commits, it broadcasts a {@link MessageType#COMMIT}
 *       with the same zxid and value so everyone can apply the operation.</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ZabPayload {
    /**
     * Indicates which stage of the Zab protocol is being performed (PROPOSAL, ACK, or COMMIT).
     */
    private MessageType type;

    /**
     * The globally unique transaction identifier for this proposal (incremented on the leader side).
     */
    private long zxid;

    /**
     * The actual data or command being replicated. In a real system, this could be
     * a serialized transaction or state machine command.
     */
    private Object proposedValue;
}