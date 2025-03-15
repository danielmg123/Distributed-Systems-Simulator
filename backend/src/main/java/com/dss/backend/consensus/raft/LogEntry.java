package com.dss.backend.consensus.raft;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a single entry in the Raft log.
 * <p>
 * Each log entry tracks:
 * <ul>
 *   <li><strong>term</strong>: the leader’s term when the entry was inserted</li>
 *   <li><strong>command</strong>: the state machine command or operation to apply once committed</li>
 * </ul>
 * <p>
 * In a future Raft implementation, these entries would typically be written to stable
 * storage for persistence across restarts.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogEntry {

    /**
     * The term number during which the leader received this entry.
     */
    private int term;

    /**
     * The actual command or operation to apply to the state machine upon commit.
     * Could be any serializable object or domain-specific data.
     */
    private Object command;
}