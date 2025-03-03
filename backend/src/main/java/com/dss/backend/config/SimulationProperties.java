package com.dss.backend.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "simulation")
public class SimulationProperties {

    // Getters and setters
    /**
     * Number of threads for the shared scheduler.
     */
    private int schedulerThreadPoolSize = 10;

    /**
     * Number of threads for the worker pool (e.g. in NodeInitializationService).
     */
    private int workerThreadPoolSize = 10;

    /**
     * Timeout for MultiPaxos prepare phase (in milliseconds).
     */
    private long multipaxosPrepareTimeoutMillis = 5000;

    /**
     * If non-zero, use this value as the quorum for MultiPaxos; otherwise compute as (totalNodes/2)+1.
     */
    private int multipaxosQuorum = 0;

    /**
     * Heartbeat interval in milliseconds.
     */
    private long heartbeatIntervalMillis = 1000;

}