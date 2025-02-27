package com.dss.backend.algorithm.failure;

import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.engine.concurrent.MessageType;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Heartbeat {
    private final MessageRouter router;
    private final String nodeId;
    // How often (in ms) to send heartbeats
    private final long heartbeatIntervalMillis = 1000;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public Heartbeat(MessageRouter router, String nodeId) {
        this.router = router;
        this.nodeId = nodeId;
    }

    // Start sending heartbeats periodically to all other nodes.
    public void startHeartbeat() {
        scheduler.scheduleAtFixedRate(() -> {
            // For every registered node (except self), send a heartbeat message.
            for (String targetId : router.getRegisteredNodeIds()) {
                if (!targetId.equals(nodeId)) {
                    // Use the current system time as the payload.
                    SimulationMessage msg = new SimulationMessage(nodeId, targetId, MessageType.HEARTBEAT, System.currentTimeMillis());
                    router.messageSent(msg);
                }
            }
        }, 0, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
    }

    public void stopHeartbeat() {
        scheduler.shutdownNow();
    }
}