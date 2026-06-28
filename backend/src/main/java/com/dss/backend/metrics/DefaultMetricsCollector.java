package com.dss.backend.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * A simple in-memory implementation that aggregates performance metrics.
 */
public class DefaultMetricsCollector implements PerformanceMetricsCollector {
    private final LongAdder messageCount = new LongAdder();
    private final LongAdder proposalCount = new LongAdder();
    private final LongAdder commitCount = new LongAdder();
    private final LongAdder droppedMessageCount = new LongAdder();

    @Override
    public void recordMessage() {
        messageCount.increment();
    }

    @Override
    public void recordProposal() {
        proposalCount.increment();
    }

    @Override
    public void recordCommit() {
        commitCount.increment();
    }

    @Override
    public void recordDroppedMessage() {
        droppedMessageCount.increment();
    }

    @Override
    public MetricsSnapshot getSnapshot() {
        return new MetricsSnapshot(
                messageCount.sum(),
                proposalCount.sum(),
                commitCount.sum(),
                droppedMessageCount.sum()
        );
    }
}