package com.dss.backend.dto;

import com.dss.backend.model.ConsensusAlgorithmType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Data Transfer Object representing a consensus algorithm available in the system.")
public class AlgorithmDTO {

    @Schema(description = "The name of the algorithm", example = "Paxos")
    private String name;

    @Schema(description = "The type of consensus algorithm", example = "PAXOS")
    private ConsensusAlgorithmType type;
}