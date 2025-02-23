package com.dss.backend.metrics;

import lombok.Getter;

/**
 * Data class representing the estimated performance metrics for a full scale simulation.
 */
@Getter
public class FullScalePerformanceEstimation {
    private final double estimatedAverageLatencyMillis;
    private final double estimatedThroughput; // proposals/commits per unit time (as an example)
    private final double estimatedFailureRecoveryTimeMillis;

    public FullScalePerformanceEstimation(double estimatedAverageLatencyMillis, double estimatedThroughput, double estimatedFailureRecoveryTimeMillis) {
        this.estimatedAverageLatencyMillis = estimatedAverageLatencyMillis;
        this.estimatedThroughput = estimatedThroughput;
        this.estimatedFailureRecoveryTimeMillis = estimatedFailureRecoveryTimeMillis;
    }

    @Override
    public String toString() {
        return "FullScalePerformanceEstimation{" +
                "estimatedAverageLatencyMillis=" + estimatedAverageLatencyMillis +
                ", estimatedThroughput=" + estimatedThroughput +
                ", estimatedFailureRecoveryTimeMillis=" + estimatedFailureRecoveryTimeMillis +
                '}';
    }
}