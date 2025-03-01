package com.dss.backend.algorithm.consensus.util;

import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.SimulationMessage;

public class ConsensusBroadcaster {
    private final MessageRouter router;
    private final String localNodeId;

    public ConsensusBroadcaster(MessageRouter router, String localNodeId) {
        this.router = router;
        this.localNodeId = localNodeId;
    }

    /**
     * Broadcasts a message of the given type and payload to all nodes except the local one.
     */
    public void broadcast(MessageType messageType, Object payload) {
        for (String targetNodeId : router.getRegisteredNodeIds()) {
            if (!targetNodeId.equals(localNodeId)) {
                SimulationMessage msg = new SimulationMessage(localNodeId, targetNodeId, messageType, payload);
                router.messageSent(msg);
            }
        }
    }
}
