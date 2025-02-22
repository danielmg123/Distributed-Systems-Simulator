package com.dss.backend.model;

import lombok.Data;
import java.util.List;

@Data
public class SimulationConfig {
    // Which consensus algorithm to use
    private ConsensusAlgorithmType algorithmType;

    // Total number of nodes participating in the simulation.
    private int nodeCount;

    // Network topology type
    private TopologyType topologyType;

    // Failure scenario: percentage of nodes to fail (e.g. 10 for 10%)
    private double failurePercentage;

    // List of performance metrics to capture (latency, throughput, etc..)
    private List<String> metricsToCapture;

    // Security option: true if TLS/SSL is enabled
    private boolean tlsEnabled;
}
