package com.dss.backend.algorithm.consensus;

import com.dss.backend.algorithm.consensus.paxos.PaxosAlgorithm;
import com.dss.backend.algorithm.consensus.raft.Raft;
import com.dss.backend.config.SimulationProperties;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.engine.DefaultScheduler;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.model.ConsensusAlgorithmType;
import com.dss.backend.model.SimulationConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsensusAlgorithmFactoryTest {

    private ConsensusAlgorithmFactory factory;

    @BeforeEach
    public void setUp() {
        MessageRouter router = new MessageRouter();
        Scheduler scheduler = new DefaultScheduler(Executors.newSingleThreadScheduledExecutor());
        SimulationProperties simulationProperties = new SimulationProperties();
        factory = new ConsensusAlgorithmFactory(router, scheduler, simulationProperties);

    }

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