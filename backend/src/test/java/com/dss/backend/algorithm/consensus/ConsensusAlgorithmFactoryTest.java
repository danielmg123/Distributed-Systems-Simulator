package com.dss.backend.algorithm.consensus;

import com.dss.backend.model.ConsensusAlgorithmType;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.algorithm.consensus.paxos.PaxosAlgorithm;
import com.dss.backend.algorithm.consensus.raft.Raft;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static org.junit.jupiter.api.Assertions.*;

public class ConsensusAlgorithmFactoryTest {

    private static ScheduledExecutorService scheduler;

    @BeforeAll
    public static void setupScheduler() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterAll
    public static void shutdownScheduler() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    public void testFactoryReturnsPaxosByDefault() {
        MessageRouter router = new MessageRouter();
        ConsensusAlgorithm algorithm = ConsensusAlgorithmFactory.createAlgorithm(
                "node1",
                Collections.singletonList("node1"),
                null,
                router,
                scheduler
        );
        assertTrue(algorithm instanceof PaxosAlgorithm);
    }

    @Test
    public void testFactoryReturnsRaftWhenConfigured() {
        SimulationConfig config = new SimulationConfig();
        config.setAlgorithmType(ConsensusAlgorithmType.RAFT);
        MessageRouter router = new MessageRouter();
        ConsensusAlgorithm algorithm = ConsensusAlgorithmFactory.createAlgorithm(
                "node1",
                Collections.singletonList("node1"),
                config,
                router,
                scheduler
        );
        assertTrue(algorithm instanceof Raft);
    }
}
