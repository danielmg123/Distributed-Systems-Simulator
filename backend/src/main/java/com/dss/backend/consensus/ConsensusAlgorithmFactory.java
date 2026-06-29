package com.dss.backend.consensus;

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
    private final ConsensusObserver observer;
    private final long raftElectionTimeoutMinMillis;
    private final long raftElectionTimeoutMaxMillis;

    /**
     * Constructs a factory whose algorithms report no metrics/events (observer is a no-op)
     * and whose Raft uses the default election-timeout range from {@code simulationProperties}.
     * Convenient for tests and standalone use.
     */
    public ConsensusAlgorithmFactory(MessageRouter router,
                                     Scheduler scheduler,
                                     SimulationProperties simulationProperties) {
        this(router, scheduler, simulationProperties, ConsensusObserver.NO_OP,
                simulationProperties.getRaftElectionTimeoutMinMillis(),
                simulationProperties.getRaftElectionTimeoutMaxMillis());
    }

    /**
     * Constructs a new factory with shared dependencies (MessageRouter, Scheduler, etc.)
     * needed by all algorithms, an observer that every created algorithm is wired to so it
     * can report commits (and, for Raft, leader elections), and the Raft election-timeout
     * range to use (scaled by the simulation to tolerate the configured network delay).
     *
     * @param router                    the shared {@link MessageRouter} used for message sending
     * @param scheduler                 a shared {@link Scheduler} for scheduled tasks/timeouts
     * @param simulationProperties      config properties that control timeouts, thread pools, etc.
     * @param observer                  notified of commits/leader-elections by every created algorithm
     * @param raftElectionTimeoutMinMillis minimum randomized Raft election timeout
     * @param raftElectionTimeoutMaxMillis maximum randomized Raft election timeout
     */
    public ConsensusAlgorithmFactory(MessageRouter router,
                                     Scheduler scheduler,
                                     SimulationProperties simulationProperties,
                                     ConsensusObserver observer,
                                     long raftElectionTimeoutMinMillis,
                                     long raftElectionTimeoutMaxMillis) {
        this.router = router;
        this.scheduler = scheduler;
        this.simulationProperties = simulationProperties;
        this.observer = (observer != null) ? observer : ConsensusObserver.NO_OP;
        this.raftElectionTimeoutMinMillis = raftElectionTimeoutMinMillis;
        this.raftElectionTimeoutMaxMillis = raftElectionTimeoutMaxMillis;
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
            return withObserver(new PaxosAlgorithm(nodeId, allNodeIds, router));
        }

        switch (config.getAlgorithmType()) {
            case PAXOS:
                return withObserver(new PaxosAlgorithm(nodeId, allNodeIds, router));
            case RAFT:
                return withObserver(new Raft(nodeId, allNodeIds, router, scheduler, simulationProperties,
                        raftElectionTimeoutMinMillis, raftElectionTimeoutMaxMillis));
            case MULTI_PAXOS:
                MultiPaxos multiPaxos = new MultiPaxos(nodeId, allNodeIds, router, simulationProperties, scheduler);
                multiPaxos.setTotalNodes(allNodeIds.size());
                // For simplicity, pick the first node as leader
                multiPaxos.setLeader(allNodeIds.get(0).equals(nodeId));
                return withObserver(multiPaxos);
            default:
                // Unrecognized type
                throw new IllegalArgumentException("Unsupported consensus algorithm: " + config.getAlgorithmType());
        }
    }

    /** Wires the factory's observer into a freshly-created algorithm and returns it. */
    private ConsensusAlgorithm withObserver(ConsensusAlgorithm algorithm) {
        algorithm.setConsensusObserver(observer);
        return algorithm;
    }
}