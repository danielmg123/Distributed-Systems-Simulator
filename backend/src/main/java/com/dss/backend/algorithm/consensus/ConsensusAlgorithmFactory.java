package com.dss.backend.algorithm.consensus;

import com.dss.backend.algorithm.consensus.view_stamped_replication.ViewStampedReplication;
import com.dss.backend.algorithm.consensus.zab.Zab;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.algorithm.consensus.paxos.PaxosAlgorithm;
import com.dss.backend.algorithm.consensus.raft.Raft;
import com.dss.backend.algorithm.consensus.multi_paxos.MultiPaxos;
import com.dss.backend.engine.concurrent.MessageRouter;
import java.util.List;

public class ConsensusAlgorithmFactory {

    private final MessageRouter router;
    private final Scheduler scheduler;

    public ConsensusAlgorithmFactory(MessageRouter router, Scheduler scheduler) {
        this.router = router;
        this.scheduler = scheduler;
    }

    /**
     * Creates a ConsensusAlgorithm based on configuration. Note that the shared MessageRouter and Scheduler
     * are injected into the factory.
     *
     * @param nodeId     Local node ID.
     * @param allNodeIds List of all node IDs.
     * @param config     Simulation configuration.
     * @return a configured ConsensusAlgorithm.
     */
    public ConsensusAlgorithm createAlgorithm(String nodeId, List<String> allNodeIds, SimulationConfig config) {
        if (config == null || config.getAlgorithmType() == null) {
            return new PaxosAlgorithm(nodeId, allNodeIds, router);
        }

        switch (config.getAlgorithmType()) {
            case PAXOS:
                return new PaxosAlgorithm(nodeId, allNodeIds, router);
            case RAFT:
                return new Raft(nodeId, allNodeIds, router);
            case MULTI_PAXOS:
                MultiPaxos multiPaxos = new MultiPaxos();
                multiPaxos.setMessageRouter(router);
                multiPaxos.setTotalNodes(allNodeIds.size());
                multiPaxos.setLeader(allNodeIds.get(0).equals(nodeId));
                multiPaxos.setScheduler(scheduler);
                return multiPaxos;
            case VIEW_STAMPED_REPLICATION:
                ViewStampedReplication vsr = new ViewStampedReplication();
                vsr.setMessageRouter(router);
                return vsr;
            case ZAB:
                Zab zab = new Zab();
                zab.setMessageRouter(router);
                return zab;
            default:
                throw new IllegalArgumentException("Unsupported consensus algorithm: " + config.getAlgorithmType());
        }
    }
}