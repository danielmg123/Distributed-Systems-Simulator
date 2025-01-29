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
    ACCEPTED
}