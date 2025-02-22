package com.dss.backend.algorithm.consensus;

import com.dss.backend.model.ConsensusAlgorithmType;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.algorithm.consensus.paxos.PaxosAlgorithm;
import com.dss.backend.algorithm.consensus.raft.Raft;
import com.dss.backend.algorithm.consensus.multi_paxos.MultiPaxos;
import com.dss.backend.engine.concurrent.MessageRouter;
import java.util.List;

public class ConsensusAlgorithmFactory {

    /**
     * Create a ConsensusAlgorithm instance based on the configuration.
     *
     * @param nodeId       The local node identifier.
     * @param allNodeIds   The list of all node identifiers.
     * @param config       The simulation configuration (may contain algorithm-specific parameters).
     * @param router       The MessageRouter used for inter node communication.
     * @return An instance of ConsensusAlgorithm.
     */
    public static ConsensusAlgorithm createAlgorithm(String nodeId, List<String> allNodeIds,
                                                     SimulationConfig config, MessageRouter router) {
        // Default to Paxos if no configuration is provided.
        if (config == null || config.getAlgorithmType() == null) {
            return new PaxosAlgorithm(nodeId, allNodeIds, router);
        }

        switch (config.getAlgorithmType()) {
            case PAXOS:
                // Can pass additional algorithm-specific parameters from config.
                return new PaxosAlgorithm(nodeId, allNodeIds, router);
            case RAFT:
                return new Raft(nodeId, allNodeIds, router);
            case MULTI_PAXOS:
                // Assuming MultiPaxos requires no additional parameters for now.
                return new MultiPaxos();
            // Additional cases for other algorithms will be added here.
            default:
                throw new IllegalArgumentException("Unsupported consensus algorithm: " + config.getAlgorithmType());
        }
    }
}