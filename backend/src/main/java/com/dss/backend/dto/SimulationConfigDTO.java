package com.dss.backend.dto;

import lombok.Data;
import java.util.List;
import com.dss.backend.model.ConsensusAlgorithmType;
import com.dss.backend.model.TopologyType;

@Data
public class SimulationConfigDTO {
    private ConsensusAlgorithmType algorithmType;
    private int nodeCount;
    private TopologyType topologyType;
    private double failurePercentage;
    private List<String> metricsToCapture;
    private boolean tlsEnabled;
}
