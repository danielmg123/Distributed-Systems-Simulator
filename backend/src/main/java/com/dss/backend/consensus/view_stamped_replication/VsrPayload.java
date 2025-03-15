package com.dss.backend.consensus.view_stamped_replication;

import com.dss.backend.messaging.MessageType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The payload carried by View Stamped Replication (VSR) messages.
 * It captures the information needed in each phase (PREPARE, PREPARE_RESPONSE, COMMIT).
 *
 * <p><strong>Fields:</strong></p>
 * <ul>
 *   <li><strong>type</strong>: Indicates the VSR message type ({@link MessageType#PREPARE},
 *       {@link MessageType#PREPARE_RESPONSE}, or {@link MessageType#COMMIT}).</li>
 *   <li><strong>view</strong>: Identifies the current view number for the replication group.
 *       In normal operation, all messages are associated with the same {@code view}
 *       until a view change occurs.</li>
 *   <li><strong>opNum</strong>: Unique operation number within the current view. Increments
 *       for each new client request that the primary proposes.</li>
 *   <li><strong>proposedValue</strong>: The actual command or data being replicated.</li>
 * </ul>
 *
 * <p><strong>Usage:</strong></p>
 * <ul>
 *   <li>A node (primary) constructing a {@code PREPARE} message sets {@code opNum, view, proposedValue}
 *       and broadcasts it to backups.</li>
 *   <li>Backups respond with a {@code PREPARE_RESPONSE} using the same {@code opNum}
 *       (and typically the same {@code view}).</li>
 *   <li>Once the primary receives enough acknowledgments, it sends a {@code COMMIT}
 *       so that all nodes finalize the operation.</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VsrPayload {
    /**
     * The type of VSR message (PREPARE, PREPARE_RESPONSE, COMMIT).
     */
    private MessageType type;

    /**
     * The current view number. All nodes share this value unless a view change occurs.
     */
    private int view;

    /**
     * The operation sequence number (unique per proposal in this view).
     */
    private int opNum;

    /**
     * The proposed value or command. Used by PREPARE, echoed in PREPARE_RESPONSE,
     * and re-used in COMMIT messages.
     */
    private Object proposedValue;
}