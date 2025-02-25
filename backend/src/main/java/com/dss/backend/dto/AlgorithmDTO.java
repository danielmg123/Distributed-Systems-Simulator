package com.dss.backend.dto;

import com.dss.backend.model.ConsensusAlgorithmType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AlgorithmDTO {
    private String name;
    private ConsensusAlgorithmType type;
}