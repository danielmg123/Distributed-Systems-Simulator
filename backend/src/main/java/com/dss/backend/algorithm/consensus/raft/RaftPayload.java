package com.dss.backend.algorithm.consensus.raft;

import com.dss.backend.engine.concurrent.MessageType;
import lombok.Data;

import java.util.List;

@Data
public class RaftPayload {
    private MessageType type;
    private int term;
    private String candidateId; 
    private String leaderId;

    // For log replication
    private List<LogEntry> entries;     // The new log entries to store (can be empty for heartbeat)
    private int prevLogIndex;          // Index of log entry immediately preceding new ones
    private int prevLogTerm;           // Term of prevLogIndex entry
    private int leaderCommit;          // Leader’s commitIndex

    // For responses or votes
    private boolean success;           
    private boolean voteGranted;

    // Possibly: index of the last entry appended, or mismatch hints, etc.
    private int matchIndex; 
    private int conflictIndex;         
    private int conflictTerm;          
}