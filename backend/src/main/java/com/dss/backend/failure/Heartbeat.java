package com.dss.backend.failure;

import com.dss.backend.engine.Scheduler;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.ProtocolType;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Heartbeat implements HeartbeatService {
    private final MessageRouter router;
    private final String nodeId;
    private final long heartbeatIntervalMillis;
    private ScheduledFuture<?> heartbeatFuture;

    public Heartbeat(MessageRouter router, String nodeId, long heartbeatIntervalMillis) {
        this.router = router;
        this.nodeId = nodeId;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    @Override
    public void start(Scheduler scheduler) {
        try {
            // Check if the scheduler is shutdown before scheduling
            if (schedulerIsShutdown(scheduler)) {
                return;
            }
            heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
                if (schedulerIsShutdown(scheduler)) {
                    return;
                }
                for (String targetId : router.getRegisteredNodeIds()) {
                    if (!targetId.equals(nodeId)) {
                        SimulationMessage msg = new SimulationMessage(
                                nodeId,
                                targetId,
                                MessageType.HEARTBEAT,
                                System.currentTimeMillis(),
                                ProtocolType.UNIVERSAL
                        );
                        router.messageSent(msg);
                    }
                }
            }, 0, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ex) {
            // Scheduler is shut down or terminating. Optionally log the event.
        }
    }

    private boolean schedulerIsShutdown(Scheduler scheduler) {
        if (scheduler instanceof ScheduledThreadPoolExecutor) {
            return ((ScheduledThreadPoolExecutor) scheduler).isShutdown();
        }
        return false;
    }

    @Override
    public void stop() {
        if (heartbeatFuture != null && !heartbeatFuture.isCancelled()) {
            heartbeatFuture.cancel(true);
        }
    }
}