package com.dss.backend.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import java.util.List;
import com.dss.backend.model.ConsensusAlgorithmType;
import com.dss.backend.model.TopologyType;

@Data
@Schema(description = "Data Transfer Object for simulation configuration details.")
public class SimulationConfigDTO {

    @NotNull(message = "is required")
    @Schema(description = "Type of consensus algorithm to be used", example = "PAXOS")
    private ConsensusAlgorithmType algorithmType;

    @Schema(description = "Number of nodes participating in the simulation", example = "5")
    private int nodeCount;

    @NotNull(message = "is required")
    @Schema(description = "Type of network topology", example = "MESH")
    private TopologyType topologyType;

    @Schema(description = "Percentage of nodes to fail during simulation", example = "10.0")
    private double failurePercentage;

    @Schema(description = "List of metrics to capture during the simulation", example = "[\"latency\", \"throughput\"]")
    private List<String> metricsToCapture;

    @Schema(description = "Indicates if TLS/SSL is enabled", example = "false")
    private boolean tlsEnabled;

    @Schema(description = "Probability (0.0-1.0) that any given message is randomly dropped in transit, " +
            "independent of node failures. 0 (default) disables random loss.", example = "0.0")
    private double messageLossRate;

    @Schema(description = "Minimum simulated message delivery delay, in milliseconds. Has no effect unless " +
            "maxMessageDelayMs is also set to a positive value.", example = "0")
    private long minMessageDelayMs;

    @Schema(description = "Maximum simulated message delivery delay, in milliseconds. 0 (default) disables " +
            "delay simulation entirely, delivering messages synchronously.", example = "0")
    private long maxMessageDelayMs;
}