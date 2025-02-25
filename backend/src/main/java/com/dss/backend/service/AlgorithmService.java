package com.dss.backend.service;

import com.dss.backend.dto.AlgorithmDTO;
import com.dss.backend.model.ConsensusAlgorithmType;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service
public class AlgorithmService {

    public List<AlgorithmDTO> getAvailableAlgorithms() {
        return Arrays.asList(
                new AlgorithmDTO("Paxos", ConsensusAlgorithmType.PAXOS),
                new AlgorithmDTO("Raft", ConsensusAlgorithmType.RAFT),
                new AlgorithmDTO("Multi-Paxos", ConsensusAlgorithmType.MULTI_PAXOS),
                new AlgorithmDTO("ViewStampedReplication", ConsensusAlgorithmType.VIEW_STAMPED_REPLICATION),
                new AlgorithmDTO("Zab", ConsensusAlgorithmType.ZAB)
        );
    }
}
