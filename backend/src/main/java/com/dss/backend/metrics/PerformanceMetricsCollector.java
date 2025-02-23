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
     * Returns a snapshot of the aggregated metrics.
     */
    MetricsSnapshot getSnapshot();
}