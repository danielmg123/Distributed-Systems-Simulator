package com.dss.backend.failure;

import com.dss.backend.engine.Scheduler;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.MessageType;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Heartbeat implements HeartbeatService {
    private final MessageRouter router;
    private final String nodeId;
    private final long heartbeatIntervalMillis; // now configurable
    private ScheduledFuture<?> heartbeatFuture;

    public Heartbeat(MessageRouter router, String nodeId, long heartbeatIntervalMillis) {
        this.router = router;
        this.nodeId = nodeId;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    // Schedule heartbeat tasks on the provided central scheduler.
    @Override
    public void start(Scheduler scheduler) {
        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
            for (String targetId : router.getRegisteredNodeIds()) {
                if (!targetId.equals(nodeId)) {
                    SimulationMessage msg = new SimulationMessage(nodeId, targetId, MessageType.HEARTBEAT, System.currentTimeMillis());
                    router.messageSent(msg);
                }
            }
        }, 0, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (heartbeatFuture != null && !heartbeatFuture.isCancelled()) {
            heartbeatFuture.cancel(true);
        }
    }
}