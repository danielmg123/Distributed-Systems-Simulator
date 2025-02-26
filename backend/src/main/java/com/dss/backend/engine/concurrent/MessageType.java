package com.dss.backend.engine.concurrent;

public enum MessageType {
    // universal types
    PROPOSAL, 
    ACCEPT, 
    COMMIT, 
    HEARTBEAT, 
    FAILURE, 
    RECOVERY,

    // Paxos-specific types
    PREPARE_REQUEST,
    PROMISE,
    ACCEPT_REQUEST,
    ACCEPTED,

    // Raft-specific types
    REQUEST_VOTE,
    REQUEST_VOTE_RESPONSE,
    APPEND_ENTRIES,
    APPEND_ENTRIES_RESPONSE,

    // ViewStamped Replication-specific types
    PREPARE,
    PREPARE_RESPONSE,

    // ZooKeeper Atomic Broadcast-specific types
    ACK
}