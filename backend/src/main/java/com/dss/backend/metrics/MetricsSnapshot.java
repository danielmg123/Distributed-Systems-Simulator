package com.dss.backend.metrics;

import lombok.Getter;

/**
 * A simple data class to capture a snapshot of performance metrics.
 */
@Getter
public class MetricsSnapshot {
    private final long totalMessages;
    private final long totalLatencyMillis;
    private final long totalProposals;
    private final long totalCommits;
    private final long totalFailureRecoveryMillis;

    public MetricsSnapshot(long totalMessages, long totalLatencyMillis, long totalProposals, long totalCommits, long totalFailureRecoveryMillis) {
        this.totalMessages = totalMessages;
        this.totalLatencyMillis = totalLatencyMillis;
        this.totalProposals = totalProposals;
        this.totalCommits = totalCommits;
        this.totalFailureRecoveryMillis = totalFailureRecoveryMillis;
    }

    @Override
    public String toString() {
        return "MetricsSnapshot{" +
                "totalMessages=" + totalMessages +
                ", totalLatencyMillis=" + totalLatencyMillis +
                ", totalProposals=" + totalProposals +
                ", totalCommits=" + totalCommits +
                ", totalFailureRecoveryMillis=" + totalFailureRecoveryMillis +
                '}';
    }
}
