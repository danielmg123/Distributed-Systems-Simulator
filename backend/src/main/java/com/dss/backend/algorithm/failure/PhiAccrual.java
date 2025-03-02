package com.dss.backend.algorithm.failure;

import java.util.LinkedList;
import java.util.Queue;

public class PhiAccrual implements FailureDetector{
    private final Queue<Long> heartbeatIntervals = new LinkedList<>();
    private long lastHeartbeatTime = 0;
    private final int sampleSize = 100; // maximum number of samples to keep

    // Call this method whenever a heartbeat is received (with the current time in ms)
    @Override
    public void recordHeartbeat(long currentTimeMillis) {
        if (lastHeartbeatTime != 0) {
            long interval = currentTimeMillis - lastHeartbeatTime;
            if (heartbeatIntervals.size() >= sampleSize) {
                heartbeatIntervals.poll();
            }
            heartbeatIntervals.offer(interval);
        }
        lastHeartbeatTime = currentTimeMillis;
    }

    // Compute the phi value based on the exponential distribution of intervals.
    @Override
    public double computePhi(long currentTimeMillis) {
        if (heartbeatIntervals.isEmpty()) {
            return 0.0;
        }
        long delta = currentTimeMillis - lastHeartbeatTime;
        // Calculate the mean interval.
        double mean = heartbeatIntervals.stream().mapToLong(Long::longValue).average().orElse(0);
        if (mean == 0) {
            return 0.0;
        }
        // Using the exponential distribution: P(delay >= delta) = exp(-delta/mean)
        double probability = Math.exp(-((double) delta / mean));
        // phi is defined as: -log10(P)
        return -Math.log10(probability);
    }
}
