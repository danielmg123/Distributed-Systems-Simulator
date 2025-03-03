package com.dss.backend.algorithm.consensus;

import com.dss.backend.algorithm.consensus.paxos.PaxosAlgorithm;
import com.dss.backend.algorithm.consensus.raft.Raft;
import com.dss.backend.model.ConsensusAlgorithmType;
import com.dss.backend.model.SimulationConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class ConsensusAlgorithmFactoryTest {

    @Autowired
    private ConsensusAlgorithmFactory factory; // Let Spring inject it

    @Test
    public void testFactoryReturnsPaxosByDefault() {
        ConsensusAlgorithm algorithm = factory.createAlgorithm(
                "node1",
                Collections.singletonList("node1"),
                null
        );
        assertTrue(algorithm instanceof PaxosAlgorithm);
    }

    @Test
    public void testFactoryReturnsRaftWhenConfigured() {
        SimulationConfig config = new SimulationConfig();
        config.setAlgorithmType(ConsensusAlgorithmType.RAFT);

        ConsensusAlgorithm algorithm = factory.createAlgorithm(
                "node1",
                Collections.singletonList("node1"),
                config
        );
        assertTrue(algorithm instanceof Raft);
    }
}
