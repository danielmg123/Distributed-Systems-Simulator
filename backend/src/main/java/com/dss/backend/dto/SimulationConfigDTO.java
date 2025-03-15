package com.dss.backend.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import java.util.List;
import com.dss.backend.model.ConsensusAlgorithmType;
import com.dss.backend.model.TopologyType;

@Data
@Schema(description = "Data Transfer Object for simulation configuration details.")
public class SimulationConfigDTO {

    @Schema(description = "Type of consensus algorithm to be used", example = "PAXOS")
    private ConsensusAlgorithmType algorithmType;

    @Schema(description = "Number of nodes participating in the simulation", example = "5")
    private int nodeCount;

    @Schema(description = "Type of network topology", example = "MESH")
    private TopologyType topologyType;

    @Schema(description = "Percentage of nodes to fail during simulation", example = "10.0")
    private double failurePercentage;

    @Schema(description = "List of metrics to capture during the simulation", example = "[\"latency\", \"throughput\"]")
    private List<String> metricsToCapture;

    @Schema(description = "Indicates if TLS/SSL is enabled", example = "false")
    private boolean tlsEnabled;
}