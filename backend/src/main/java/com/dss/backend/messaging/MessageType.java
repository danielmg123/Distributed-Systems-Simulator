package com.dss.backend.messaging;

public enum MessageType {
    // universal types
    PROPOSAL,
    COMMIT,
    HEARTBEAT,
    FAILURE,
    RECOVERY,

    // node-local control signal: a client proposal injected onto a node's own inbound
    // queue so it is handled on the node's single message-processing thread rather than
    // on the caller's thread (see VirtualNode.propose). Never sent over the router.
    PROPOSE,

    // Paxos-specific types
    PREPARE_REQUEST,
    PROMISE,
    ACCEPT_REQUEST,
    ACCEPTED,

    // Raft-specific types
    REQUEST_VOTE,
    REQUEST_VOTE_RESPONSE,
    APPEND_ENTRIES,
    APPEND_ENTRIES_RESPONSE
}