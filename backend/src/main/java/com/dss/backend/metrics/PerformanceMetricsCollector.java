package com.dss.backend.metrics;

/**
 * Interface for recording performance metrics.
 */
public interface PerformanceMetricsCollector {
    void recordMessageLatency(long latencyMillis);
    void recordProposal();
    void recordCommit();
    void recordFailureRecoveryTime(long recoveryMillis);

    /**
     * Records that a message was dropped rather than delivered, e.g. because its
     * source or target node was FAILED at the time {@code MessageRouter.messageSent()}
     * was called.
     */
    void recordDroppedMessage();

    /**
     * Returns a snapshot of the aggregated metrics.
     */
    MetricsSnapshot getSnapshot();
}