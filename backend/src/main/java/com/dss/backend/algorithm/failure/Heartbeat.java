package com.dss.backend.algorithm.failure;

import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.engine.concurrent.MessageType;
import java.util.concurrent.ScheduledFuture;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Heartbeat {
    private final MessageRouter router;
    private final String nodeId;
    // How often (in ms) to send heartbeats
    private final long heartbeatIntervalMillis = 1000;
    private ScheduledFuture<?> heartbeatFuture;

    public Heartbeat(MessageRouter router, String nodeId) {
        this.router = router;
        this.nodeId = nodeId;
    }

    // Schedule heartbeat tasks on the provided central scheduler.
    public void startHeartbeat(ScheduledExecutorService scheduler) {
        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
            for (String targetId : router.getRegisteredNodeIds()) {
                if (!targetId.equals(nodeId)) {
                    SimulationMessage msg = new SimulationMessage(nodeId, targetId, MessageType.HEARTBEAT, System.currentTimeMillis());
                    router.messageSent(msg);
                }
            }
        }, 0, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
    }

    public void stopHeartbeat() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(true);
        }
    }
}