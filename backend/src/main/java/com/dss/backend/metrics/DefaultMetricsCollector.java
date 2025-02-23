package com.dss.backend.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * A simple in-memory implementation that aggregates performance metrics.
 */
public class DefaultMetricsCollector implements PerformanceMetricsCollector {
    private final LongAdder messageCount = new LongAdder();
    private final LongAdder totalLatency = new LongAdder();
    private final LongAdder proposalCount = new LongAdder();
    private final LongAdder commitCount = new LongAdder();
    private final LongAdder failureRecoveryTime = new LongAdder();

    @Override
    public void recordMessageLatency(long latencyMillis) {
        messageCount.increment();
        totalLatency.add(latencyMillis);
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
    public void recordFailureRecoveryTime(long recoveryMillis) {
        failureRecoveryTime.add(recoveryMillis);
    }

    @Override
    public MetricsSnapshot getSnapshot() {
        return new MetricsSnapshot(
                messageCount.sum(),
                totalLatency.sum(),
                proposalCount.sum(),
                commitCount.sum(),
                failureRecoveryTime.sum()
        );
    }
}