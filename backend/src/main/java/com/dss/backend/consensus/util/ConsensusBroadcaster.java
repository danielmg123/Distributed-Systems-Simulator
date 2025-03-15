package com.dss.backend.consensus.util;

import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.ProtocolType;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.SimulationMessageFactory;

/**
 * Utility class to broadcast protocol messages to all nodes in the cluster
 * except the local node.
 * <p>
 * Useful in consensus algorithms, whenever the leader
 * or proposer needs to send a message (e.g., PREPARE_REQUEST, APPEND_ENTRIES)
 * to every other participant.
 */
public class ConsensusBroadcaster {

    private final MessageRouter router;
    private final String localNodeId;

    /**
     * Constructs a broadcaster with the shared {@link MessageRouter} and the local node’s ID.
     *
     * @param router      The shared message router for delivering messages.
     * @param localNodeId The identifier for this local node, used to skip self-broadcast.
     */
    public ConsensusBroadcaster(MessageRouter router, String localNodeId) {
        this.router = router;
        this.localNodeId = localNodeId;
    }

    /**
     * Broadcasts a message with the specified type, payload, and protocol
     * to all registered nodes except this local node.
     * <p>
     * This is typically called by the leader or by a proposer
     * to disseminate new proposals, requests, or heartbeats.
     *
     * @param messageType The type of message (e.g., PREPARE_REQUEST, ACCEPT_REQUEST, APPEND_ENTRIES)
     * @param payload     The data (proposal info, log entries, etc.)
     * @param protocol    The consensus protocol (PAXOS, RAFT, etc.)
     */
    public void broadcast(MessageType messageType, Object payload, ProtocolType protocol) {
        for (String targetNodeId : router.getRegisteredNodeIds()) {
            if (!targetNodeId.equals(localNodeId)) {
                SimulationMessage msg = SimulationMessageFactory.createMessage(
                        localNodeId, targetNodeId, messageType, payload, protocol
                );
                router.messageSent(msg);
            }
        }
    }
}