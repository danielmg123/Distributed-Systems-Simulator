package com.dss.backend.service;

import com.dss.backend.dto.AlgorithmDTO;
import com.dss.backend.model.ConsensusAlgorithmType;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class AlgorithmServiceTests {

    private final AlgorithmService algorithmService = new AlgorithmService();

    @Test
    public void getAvailableAlgorithms_ReturnsListOfAlgorithmDTO() {
        List<AlgorithmDTO> algorithms = algorithmService.getAvailableAlgorithms();
        assertNotNull(algorithms);
        assertFalse(algorithms.isEmpty());

        AlgorithmDTO first = algorithms.get(0);
        assertEquals("Paxos", first.getName());
        assertEquals(ConsensusAlgorithmType.PAXOS, first.getType());
    }
}