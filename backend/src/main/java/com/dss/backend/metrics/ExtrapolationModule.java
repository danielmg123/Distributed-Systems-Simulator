package com.dss.backend.metrics;

/**
 * Given a snapshot of metrics from a scaled-down simulation and a scaling factor,
 * extrapolate full-scale performance metrics.
 */
public interface ExtrapolationModule {
    /**
     * Extrapolates full-scale performance based on scaled metrics.
     *
     * @param scaledMetrics The metrics captured from the scaled simulation.
     * @param scalingFactor The factor by which the simulation scale is increased.
     *                      (For example: if ran with 10 nodes but plan for 100 nodes,
     *                      the scalingFactor would be 10.)
     * @return A FullScalePerformanceEstimation containing the estimated full-scale metrics.
     */
    FullScalePerformanceEstimation extrapolateFullScalePerformance(MetricsSnapshot scaledMetrics, double scalingFactor);
}
