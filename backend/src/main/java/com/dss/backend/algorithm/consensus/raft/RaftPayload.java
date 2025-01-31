package com.dss.backend.algorithm.consensus.raft;

import com.dss.backend.engine.concurrent.MessageType;

import lombok.Data;

// ------------------------------
// Minimal "RaftPayload" class
// that carries the needed fields
// for each RPC call in this skeleton
// ------------------------------
@Data
public class RaftPayload {
    private MessageType type;
    private int term;
    private String candidateId;
    private String leaderId;

    // For log replication
    private Object entry; // pretend we have a single log entry
    private boolean success; // used in responses
    private boolean voteGranted; // used in requestVoteResponse
}