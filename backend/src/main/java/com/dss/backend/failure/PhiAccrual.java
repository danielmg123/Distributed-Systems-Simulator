package com.dss.backend.failure;

import java.util.LinkedList;
import java.util.Queue;

/**
 * <strong>PhiAccrual</strong> implements the phi-accrual failure detection mechanism,
 * which uses a statistical model of heartbeat intervals to compute a suspicion level (&phi;).
 * <p>
 * <strong>Key Points:</strong>
 * <ul>
 *   <li>Maintains a moving window (or queue) of recent heartbeat intervals.</li>
 *   <li>Calculates the mean of these intervals to form an exponential distribution assumption.</li>
 *   <li>&phi; is derived from <code>-log10( <i>p</i> )</code>, where <code><i>p</i></code>
 *       is the probability that the next heartbeat would arrive after the currently observed delay.</li>
 * </ul>
 *
 * <p>When &phi; exceeds a threshold (e.g. 8.0), the node is considered suspected of failure.</p>
 */
public class PhiAccrual implements FailureDetector {

    /**
     * Holds the most recent heartbeat intervals in milliseconds.
     */
    private final Queue<Long> heartbeatIntervals = new LinkedList<>();

    /**
     * The timestamp (in ms) of the most recent heartbeat. Initially {@code 0} until the first heartbeat arrives.
     */
    private long lastHeartbeatTime = 0;

    /**
     * Maximum number of intervals to keep in the rolling window of samples.
     */
    private final int sampleSize = 100;

    /**
     * Records a new heartbeat occurrence.
     * <p>
     * <strong>Logic:</strong>
     * If we have a previous heartbeat time, calculate the interval since that time
     * and store it in a fixed-size queue. Then update {@code lastHeartbeatTime}.
     */
    @Override
    public void recordHeartbeat(long currentTimeMillis) {
        if (lastHeartbeatTime != 0) {
            long interval = currentTimeMillis - lastHeartbeatTime;
            if (heartbeatIntervals.size() >= sampleSize) {
                heartbeatIntervals.poll(); // drop oldest interval
            }
            heartbeatIntervals.offer(interval);
        }
        lastHeartbeatTime = currentTimeMillis;
    }

    /**
     * Computes the phi (&phi;) value using an exponential distribution model.
     * <p>
     * <strong>Mathematical Explanation:</strong>
     * <ul>
     *   <li>Let <code>&Delta; = currentTimeMillis - lastHeartbeatTime</code> be the time since the last heartbeat.</li>
     *   <li>Compute the mean <code>&mu;</code> of the observed intervals in {@link #heartbeatIntervals}.</li>
     *   <li>Probability <code>p(delay >= &Delta;) = exp(-&Delta; / &mu;)</code> under an exponential distribution assumption.</li>
     *   <li>&phi; is defined as <code>-log10(p)</code>, so <code>&phi; = -log10(exp(-&Delta; / &mu;))</code>.</li>
     *   <li>Hence, <code>&phi; = (<&Delta; / &mu;) * log10(e)</code> numerically,
     *       but often computed via <code>Math.exp</code> and <code>Math.log10</code> directly.</li>
     * </ul>
     * If no intervals have been recorded yet, returns 0.0 to indicate insufficient data.
     *
     * @param currentTimeMillis current system time in ms for the evaluation.
     * @return the phi value, which generally increases as the time since last heartbeat grows larger than the mean interval.
     */
    @Override
    public double computePhi(long currentTimeMillis) {
        if (heartbeatIntervals.isEmpty()) {
            // No data yet, so we cannot suspect anything
            return 0.0;
        }

        long delta = currentTimeMillis - lastHeartbeatTime;
        // Compute mean interval (µ).
        double mean = heartbeatIntervals.stream().mapToLong(Long::longValue).average().orElse(0);

        if (mean == 0) {
            return 0.0;
        }

        // Probability that we would see an interval >= delta under an exponential distribution:
        // p = exp(-delta / mean)
        double p = Math.exp(-((double) delta / mean));

        // phi = -log10(p).
        // Larger phi => higher suspicion that the node has failed.
        return -Math.log10(p);
    }
}