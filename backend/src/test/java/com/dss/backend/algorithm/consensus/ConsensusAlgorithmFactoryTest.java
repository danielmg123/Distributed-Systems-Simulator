package com.dss.backend.algorithm.consensus;

import com.dss.backend.model.ConsensusAlgorithmType;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.algorithm.consensus.paxos.PaxosAlgorithm;
import com.dss.backend.algorithm.consensus.raft.Raft;
import org.junit.jupiter.api.Test;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;

public class ConsensusAlgorithmFactoryTest {

    @Test
    public void testFactoryReturnsPaxosByDefault() {
        // When no config is provided, the factory should default to Paxos.
        MessageRouter router = new MessageRouter();
        ConsensusAlgorithm algorithm = ConsensusAlgorithmFactory.createAlgorithm("node1",
                Collections.singletonList("node1"), null, router);
        assertTrue(algorithm instanceof PaxosAlgorithm);
    }

    @Test
    public void testFactoryReturnsRaftWhenConfigured() {
        SimulationConfig config = new SimulationConfig();
        config.setAlgorithmType(ConsensusAlgorithmType.RAFT);

        MessageRouter router = new MessageRouter();
        ConsensusAlgorithm algorithm = ConsensusAlgorithmFactory.createAlgorithm("node1",
                Collections.singletonList("node1"), config, router);
        assertTrue(algorithm instanceof Raft);
    }
}
