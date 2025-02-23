package com.dss.backend.metrics;

/**
 * A stub implementation of the ExtrapolationModule. In this example I use simple
 * linear scaling formulas as placeholders. In a real system these formulas would
 * incorporate baseline overheads, coefficients, and topology‐aware factors.
 */
public class DefaultExtrapolationModule implements ExtrapolationModule {

    @Override
    public FullScalePerformanceEstimation extrapolateFullScalePerformance(MetricsSnapshot scaledMetrics, double scalingFactor) {
        // Compute average latency from the scaled simulation.
        double avgLatency = scaledMetrics.getTotalMessages() > 0 ?
                (double) scaledMetrics.getTotalLatencyMillis() / scaledMetrics.getTotalMessages() : 0;
        // For demonstration, assume latency scales linearly.
        double estimatedLatency = avgLatency * scalingFactor;

        // Throughput could be computed as (proposals or commits per unit time).
        // Here I use a simple placeholder: multiply proposal count by scaling factor.
        double estimatedThroughput = scaledMetrics.getTotalProposals() * scalingFactor;

        // Assume failure recovery time scales similarly to latency (placeholder logic).
        double avgRecoveryTime = scaledMetrics.getTotalMessages() > 0 ?
                (double) scaledMetrics.getTotalFailureRecoveryMillis() / scaledMetrics.getTotalMessages() : 0;
        double estimatedRecoveryTime = avgRecoveryTime * scalingFactor;

        return new FullScalePerformanceEstimation(estimatedLatency, estimatedThroughput, estimatedRecoveryTime);
    }
}