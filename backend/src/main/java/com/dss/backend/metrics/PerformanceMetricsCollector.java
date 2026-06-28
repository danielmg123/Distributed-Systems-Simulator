package com.dss.backend.metrics;

/**
 * Interface for recording performance metrics.
 */
public interface PerformanceMetricsCollector {
    /** Records that one message was delivered to its target node. */
    void recordMessage();

    /** Records that a client proposal was submitted to the cluster. */
    void recordProposal();

    /** Records that the cluster committed/chose one value (counted once per agreed value). */
    void recordCommit();

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