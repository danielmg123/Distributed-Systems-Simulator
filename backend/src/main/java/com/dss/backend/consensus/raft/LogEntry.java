package com.dss.backend.consensus.raft;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Each Raft log entry holds:
 *   - The term number when the entry was received by the leader
 *   - The actual command (Object) to apply once committed
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogEntry {
    private int term;
    private Object command;
}