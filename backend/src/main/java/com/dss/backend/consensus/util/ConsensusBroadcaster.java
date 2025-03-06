package com.dss.backend.consensus.util;

import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.ProtocolType;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.SimulationMessageFactory;

public class ConsensusBroadcaster {
    private final MessageRouter router;
    private final String localNodeId;

    public ConsensusBroadcaster(MessageRouter router, String localNodeId) {
        this.router = router;
        this.localNodeId = localNodeId;
    }

    /**
     * Broadcasts a message of the given type, payload, and protocol to all nodes except the local one.
     */
    public void broadcast(MessageType messageType, Object payload, ProtocolType protocol) {
        for (String targetNodeId : router.getRegisteredNodeIds()) {
            if (!targetNodeId.equals(localNodeId)) {
                SimulationMessage msg = SimulationMessageFactory.createMessage(localNodeId, targetNodeId,
                        messageType, payload, protocol);
                router.messageSent(msg);
            }
        }
    }
}