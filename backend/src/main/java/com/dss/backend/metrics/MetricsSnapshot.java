package com.dss.backend.metrics;

import lombok.Getter;

/**
 * A simple data class to capture a snapshot of performance metrics.
 */
@Getter
public class MetricsSnapshot {
    private final long totalMessages;
    private final long totalProposals;
    private final long totalCommits;
    private final long totalDroppedMessages;

    public MetricsSnapshot(long totalMessages, long totalProposals, long totalCommits, long totalDroppedMessages) {
        this.totalMessages = totalMessages;
        this.totalProposals = totalProposals;
        this.totalCommits = totalCommits;
        this.totalDroppedMessages = totalDroppedMessages;
    }

    @Override
    public String toString() {
        return "MetricsSnapshot{" +
                "totalMessages=" + totalMessages +
                ", totalProposals=" + totalProposals +
                ", totalCommits=" + totalCommits +
                ", totalDroppedMessages=" + totalDroppedMessages +
                '}';
    }
}
