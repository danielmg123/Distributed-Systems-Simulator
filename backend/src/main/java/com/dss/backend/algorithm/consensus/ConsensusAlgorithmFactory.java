package com.dss.backend.algorithm.consensus;

import com.dss.backend.algorithm.consensus.view_stamped_replication.ViewStampedReplication;
import com.dss.backend.algorithm.consensus.zab.Zab;
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
                MultiPaxos multiPaxos = new MultiPaxos();
                multiPaxos.setMessageRouter(router);
                multiPaxos.setTotalNodes(allNodeIds.size());
                // First node as leader
                multiPaxos.setLeader(allNodeIds.get(0).equals(nodeId));
                return multiPaxos;
            case VIEW_STAMPED_REPLICATION:
                ViewStampedReplication vsr = new ViewStampedReplication();
                vsr.setMessageRouter(router);
                // Additional initialization (nodeId, totalNodes, primary flag) should be done after instantiation.
                return vsr;
            case ZAB:
                Zab zab = new Zab();
                zab.setMessageRouter(router);
                // Further initialization (nodeId, totalNodes, primary flag) should be done externally.
                return zab;
            default:
                throw new IllegalArgumentException("Unsupported consensus algorithm: " + config.getAlgorithmType());
        }
    }
}