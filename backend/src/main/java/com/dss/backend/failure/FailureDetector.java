package com.dss.backend.failure;

/**
 * A <strong>FailureDetector</strong> is responsible for analyzing heartbeat intervals
 * or other signals to detect when a node might have failed.
 * <p>
 * An implementation of this interface:
 * <ul>
 *   <li>{@code recordHeartbeat(long currentTimeMillis)}:
 *       Called whenever a heartbeat is received,
 *       allowing the detector to update its interval or distribution model.</li>
 *   <li>{@code computePhi(long currentTimeMillis)}:
 *       Returns a <em>phi</em> value quantifying how likely it is that the node is failed,
 *       based on the time since the last heartbeat relative to historical intervals.</li>
 * </ul>
 */
public interface FailureDetector {

    /**
     * Record the timestamp in milliseconds when a heartbeat is received.
     * <p>
     * we store the difference between this timestamp and the previous one
     * to model the distribution of heartbeat intervals over time.
     *
     * @param currentTimeMillis current system time in milliseconds when the heartbeat arrives.
     */
    void recordHeartbeat(long currentTimeMillis);

    /**
     * Compute and return the <strong>phi</strong> value given the current time.
     * <p>
     * In many implementations (e.g. phi accrual), phi is derived from the
     * cumulative distribution function (CDF) of the observed heartbeat intervals.
     * A higher phi value indicates that the heartbeat is "overdue" relative
     * to historical norms, suggesting a higher likelihood of failure.
     *
     * @param currentTimeMillis current system time in milliseconds for the evaluation.
     * @return the phi value (usually no lower than 0.0). Larger values imply a higher suspicion of failure.
     */
    double computePhi(long currentTimeMillis);
}