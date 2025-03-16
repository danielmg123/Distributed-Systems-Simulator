package com.dss.backend.failure;

import com.dss.backend.engine.Scheduler;

/**
 * <strong>HeartbeatService</strong> defines how heartbeat signals
 * are started and stopped for a node.
 * <p>
 * Typical usage in a distributed system is to keep sending heartbeats at a regular interval
 * so that other nodes can detect if this node becomes unresponsive.
 */
public interface HeartbeatService {

    /**
     * Start sending heartbeats using a provided {@link Scheduler}.
     * <p>
     * Implementations generally schedule repeated tasks that periodically send
     * heartbeat messages (or signals) to peer nodes.
     *
     * @param scheduler an external scheduler for managing timed tasks.
     */
    void start(Scheduler scheduler);

    /**
     * Stop sending heartbeats, often by canceling the scheduled task.
     * <p>
     * Once stopped, other nodes may eventually consider this node failed
     * if they receive no further heartbeats.
     */
    void stop();
}