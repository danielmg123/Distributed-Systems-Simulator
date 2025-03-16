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

/**
 * <strong>Heartbeat</strong> sends periodic heartbeat messages to other nodes
 * (via the {@link MessageRouter}) to indicate that this node is still alive.
 * <p>
 * <strong>Usage:</strong>
 * <ul>
 *   <li>Call {@link #start(Scheduler)} to begin sending heartbeats at a fixed interval.</li>
 *   <li>Call {@link #stop()} to cancel scheduled heartbeats.</li>
 * </ul>
 *
 * <p>This is often used in conjunction with a {@link FailureDetector} on the receiving nodes
 * so they can interpret delayed or missing heartbeats as potential failures.</p>
 */
public class Heartbeat implements HeartbeatService {

    private final MessageRouter router;
    private final String nodeId;
    private final long heartbeatIntervalMillis;
    private ScheduledFuture<?> heartbeatFuture;

    /**
     * Constructs a Heartbeat instance for a given node.
     *
     * @param router the shared {@link MessageRouter} for sending heartbeat messages.
     * @param nodeId the unique identifier of this node.
     * @param heartbeatIntervalMillis how often (in milliseconds) to send heartbeats.
     */
    public Heartbeat(MessageRouter router, String nodeId, long heartbeatIntervalMillis) {
        this.router = router;
        this.nodeId = nodeId;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    /**
     * Starts the recurring heartbeat task using the provided {@link Scheduler}.
     * <p>
     * Schedules a fixed-rate task that sends a {@link MessageType#HEARTBEAT} to every
     * other registered node in the system, indicating this node is alive.
     */
    @Override
    public void start(Scheduler scheduler) {
        try {
            // Protect against scheduling on a shutdown executor
            if (schedulerIsShutdown(scheduler)) {
                return;
            }
            heartbeatFuture = scheduler.scheduleAtFixedRate(
                    () -> {
                        if (schedulerIsShutdown(scheduler)) {
                            return;
                        }
                        // Send a heartbeat message to every other node
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
                    },
                    0, // no initial delay
                    heartbeatIntervalMillis,
                    TimeUnit.MILLISECONDS
            );
        } catch (RejectedExecutionException ex) {
            // Scheduler is shut down or terminating. We can log or handle this if needed.
        }
    }

    /**
     * Stops sending heartbeats by canceling the scheduled task.
     */
    @Override
    public void stop() {
        if (heartbeatFuture != null && !heartbeatFuture.isCancelled()) {
            heartbeatFuture.cancel(true);
        }
    }

    /**
     * Checks if the scheduler has been shut down to prevent scheduling or running tasks.
     *
     * @param scheduler the scheduler in use.
     * @return {@code true} if the scheduler is shut down, false otherwise.
     */
    private boolean schedulerIsShutdown(Scheduler scheduler) {
        if (scheduler instanceof ScheduledThreadPoolExecutor) {
            return ((ScheduledThreadPoolExecutor) scheduler).isShutdown();
        }
        return false;
    }
}