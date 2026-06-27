package com.dss.backend.messaging;

public enum ProtocolType {
    PAXOS,
    RAFT,
    MULTI_PAXOS,
    UNIVERSAL  // Added for messages that are not protocol-specific (e.g. HEARTBEAT)
}
