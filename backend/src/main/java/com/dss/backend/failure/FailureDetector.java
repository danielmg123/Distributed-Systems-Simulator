package com.dss.backend.failure;

public interface FailureDetector {
    /**
     * Record the timestamp (in ms) when a heartbeat is received.
     */
    void recordHeartbeat(long currentTimeMillis);

    /**
     * Compute and return the phi value given the current time.
     */
    double computePhi(long currentTimeMillis);
}
