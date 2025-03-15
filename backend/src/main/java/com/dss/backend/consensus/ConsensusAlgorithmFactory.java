package com.dss.backend.consensus;

import com.dss.backend.consensus.view_stamped_replication.ViewStampedReplication;
import com.dss.backend.consensus.zab.Zab;
import com.dss.backend.config.SimulationProperties;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.consensus.paxos.PaxosAlgorithm;
import com.dss.backend.consensus.raft.Raft;
import com.dss.backend.consensus.multi_paxos.MultiPaxos;
import com.dss.backend.messaging.MessageRouter;
import java.util.List;

/**
 * A factory class to create instances of {@link ConsensusAlgorithm} based on
 * the specified {@link com.dss.backend.model.ConsensusAlgorithmType}.
 * <p>
 * This follows the <strong>Factory Pattern</strong> so that the rest of the code
 * doesn’t need to know the concrete class details. Instead, it just requests
 * an algorithm from the factory, which decides (at runtime) which to instantiate.
 */
public class ConsensusAlgorithmFactory {

    private final MessageRouter router;
    private final Scheduler scheduler;
    private final SimulationProperties simulationProperties;

    /**
     * Constructs a new factory with shared dependencies (MessageRouter, Scheduler, etc.)
     * needed by all algorithms.
     *
     * @param router                the shared {@link MessageRouter} used for message sending
     * @param scheduler             a shared {@link Scheduler} for scheduled tasks/timeouts
     * @param simulationProperties  config properties that control aspects like timeouts, thread pools, etc.
     */
    public ConsensusAlgorithmFactory(MessageRouter router,
                                     Scheduler scheduler,
                                     SimulationProperties simulationProperties) {
        this.router = router;
        this.scheduler = scheduler;
        this.simulationProperties = simulationProperties;
    }

    /**
     * Creates and returns the appropriate {@link ConsensusAlgorithm} based on the
     * specified algorithm type in {@code config}.
     * <p>
     * <strong>Decision Logic:</strong>
     * <ul>
     *   <li>If config is null or has a null type, defaults to Paxos.</li>
     *   <li>If type is PAXOS, returns a {@link PaxosAlgorithm}.</li>
     *   <li>If RAFT, returns a {@link Raft} instance.</li>
     *   <li>If MULTI_PAXOS, returns a {@link MultiPaxos} with additional init (leader flag, etc.).</li>
     *   <li>If VIEW_STAMPED_REPLICATION, returns a {@link ViewStampedReplication} instance.</li>
     *   <li>If ZAB, returns a {@link Zab} instance.</li>
     *   <li>Otherwise, throws an {@link IllegalArgumentException} for unknown algorithm types.</li>
     * </ul>
     *
     * @param nodeId     the local node's unique ID
     * @param allNodeIds list of all node IDs in the cluster
     * @param config     simulation config holding the desired algorithm type and other settings
     * @return a new {@link ConsensusAlgorithm} instance corresponding to {@code config}'s type
     */
    public ConsensusAlgorithm createAlgorithm(String nodeId,
                                              List<String> allNodeIds,
                                              SimulationConfig config) {
        // Fallback to Paxos if config or algorithm type is null
        if (config == null || config.getAlgorithmType() == null) {
            return new PaxosAlgorithm(nodeId, allNodeIds, router);
        }

        switch (config.getAlgorithmType()) {
            case PAXOS:
                return new PaxosAlgorithm(nodeId, allNodeIds, router);
            case RAFT:
                return new Raft(nodeId, allNodeIds, router);
            case MULTI_PAXOS:
                MultiPaxos multiPaxos = new MultiPaxos(nodeId, router, simulationProperties, scheduler);
                multiPaxos.setTotalNodes(allNodeIds.size());
                // For simplicity, pick the first node as leader, or do your own logic
                multiPaxos.setLeader(allNodeIds.get(0).equals(nodeId));
                return multiPaxos;
            case VIEW_STAMPED_REPLICATION:
                ViewStampedReplication vsr = new ViewStampedReplication();
                vsr.setMessageRouter(router);
                // Additional setup for VSR if needed
                return vsr;
            case ZAB:
                Zab zab = new Zab();
                zab.setMessageRouter(router);
                // Additional setup for ZAB if needed
                return zab;
            default:
                // Unrecognized type
                throw new IllegalArgumentException("Unsupported consensus algorithm: " + config.getAlgorithmType());
        }
    }
}