package com.dss.backend.messaging;

public enum ProtocolType {
    PAXOS,
    RAFT,
    MULTI_PAXOS,
    VIEW_STAMPED_REPLICATION,
    ZAB,
    UNIVERSAL  // Added for messages that are not protocol-specific (e.g. HEARTBEAT)
}
