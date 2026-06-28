package com.dss.backend.service.engine;

import com.dss.backend.dto.NodeDTO;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.VirtualNode;
import com.dss.backend.messaging.TopologyPlacer;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.model.EventType;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.model.TopologyType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * High-level coordinator that orchestrates the simulation lifecycle:
 * <ul>
 *   <li>Initializes nodes (via {@link NodeInitializationService}).</li>
 *   <li>Starts periodic metrics updates (via {@link MetricsUpdateService}).</li>
 *   <li>Manages failure simulations (random node failures) at regular intervals.</li>
 *   <li>Stops the simulation gracefully, ensuring all nodes are shut down
 *       and scheduled tasks are canceled.</li>
 * </ul>
 *
 * <p>This class does <em>not</em> persist data or manage the simulation’s domain state
 * in a repository. Rather, it provides an in-memory coordination layer to get all
 * simulation components working in concert.
 */
public class SimulationOrchestrator {

    private final AppLogger appLogger = new DefaultAppLogger(SimulationOrchestrator.class);

    private final MessageRouter messageRouter;
    private final Scheduler scheduler;
    private final NodeInitializationService nodeInitializationService;
    private final MetricsUpdateService metricsUpdateService;
    private final EventLoggerService eventLoggerService;

    // A map from node ID -> the in-memory VirtualNode instance
    private Map<String, VirtualNode> nodeMap;

    // Neighbor-adjacency map computed from the simulation's chosen TopologyType.
    // NOTE: this is metadata for visualization only (see getTopologyMapping()) -- it is
    // not consulted by MessageRouter or by any consensus algorithm's broadcast logic,
    // which always reach every node regardless of topology.
    private Map<String, List<String>> topologyMapping;

    /**
     * Constructs an orchestrator with references to the main services used
     * for node setup, metrics, event logging, and scheduling.
     *
     * @param messageRouter            the central message router
     * @param scheduler                shared scheduler for recurring tasks
     * @param nodeInitializationService configures and starts up VirtualNodes
     * @param metricsUpdateService     manages sending metrics to the UI
     * @param eventLoggerService       records/logs notable simulation events
     */
    public SimulationOrchestrator(MessageRouter messageRouter,
                                  Scheduler scheduler,
                                  NodeInitializationService nodeInitializationService,
                                  MetricsUpdateService metricsUpdateService,
                                  EventLoggerService eventLoggerService) {
        this.messageRouter = messageRouter;
        this.scheduler = scheduler;
        this.nodeInitializationService = nodeInitializationService;
        this.metricsUpdateService = metricsUpdateService;
        this.eventLoggerService = eventLoggerService;
    }

    /**
     * Creates and starts virtual nodes for each domain {@link Node} provided.
     * If a topology is specified, it also computes neighbor relationships for
     * {@link #getTopologyMapping()} -- note that this mapping is metadata for
     * visualization only and does not affect how messages are actually routed
     * between nodes (see {@link TopologyPlacer} for details on why).
     *
     * @param nodes        the list of node definitions
     * @param config       simulation config (consensus algorithm, etc.)
     * @param topologyType ring, mesh, star, etc.
     */
    public void initializeSimulationNodes(List<Node> nodes, SimulationConfig config, TopologyType topologyType) {
        nodeMap = nodeInitializationService.initializeNodes(nodes, config, topologyType);
        if (topologyType != null) {
            topologyMapping = TopologyPlacer.assignNeighbors(topologyType, nodes);
            appLogger.info("Topology mapping: {}", topologyMapping);
        }
    }

    /**
     * Convenience method for external callers to compute a topology
     * without necessarily initializing nodes. Useful for debugging or
     * logging the ring/mesh relationships.
     *
     * @param nodes        the node list
     * @param topologyType the chosen topology type
     * @return a map of node IDs -> list of neighbor node IDs
     */
    public Map<String, List<String>> computeTopologyMapping(List<Node> nodes, TopologyType topologyType) {
        return TopologyPlacer.assignNeighbors(topologyType, nodes);
    }

    /**
     * Returns the neighbor-adjacency map computed for this simulation's topology, for
     * a dashboard to render. This is visualization metadata only -- it is not consulted
     * by {@link MessageRouter} or by any consensus algorithm's broadcast logic, which
     * always reach every registered node regardless of topology (quorum-based protocols
     * need full connectivity to function at all).
     *
     * @return a map of node ID -> list of neighbor node IDs, or an empty map if no
     *         topology was configured for this simulation
     */
    public Map<String, List<String>> getTopologyMapping() {
        return topologyMapping != null ? topologyMapping : Collections.emptyMap();
    }

    /**
     * Live-updates the simulated network conditions (random message loss and delivery
     * delay) for this simulation's {@link MessageRouter}, taking effect immediately for
     * all subsequent message sends. Intended to back UI controls (e.g. sliders) that
     * let a user tune conditions while a simulation is already running.
     *
     * @param messageLossRate probability (0.0-1.0) that a message is randomly dropped
     * @param minDelayMs      minimum simulated delivery delay, in milliseconds
     * @param maxDelayMs      maximum simulated delivery delay, in milliseconds (0 disables delay)
     */
    public void setNetworkConditions(double messageLossRate, long minDelayMs, long maxDelayMs) {
        messageRouter.setMessageLossRate(messageLossRate);
        messageRouter.setMinDelayMs(minDelayMs);
        messageRouter.setMaxDelayMs(maxDelayMs);
    }

    /**
     * Starts the simulation by initiating periodic metrics updates and
     * logging an event that the simulation has started.
     *
     * @param simulationId the unique identifier for the simulation run
     */
    public void startSimulation(String simulationId) {
        metricsUpdateService.startMetricsUpdates(simulationId);
        eventLoggerService.logEvent(simulationId, "Simulation started.", EventType.SIMULATION_STARTED);
    }

    /**
     * Periodically attempts to fail a subset of nodes at random, simulating
     * partial failures. The random chance is determined by {@code failurePercentage}.
     *
     * @param simulationId      unique ID for the simulation
     * @param failurePercentage chance (0-100) that an active node will fail on each check
     * @param intervalMillis    how often to attempt failing nodes
     */
    public void startFailureSimulation(String simulationId, double failurePercentage, int intervalMillis) {
        scheduler.scheduleAtFixedRate(() -> {
                    for (VirtualNode vNode : nodeMap.values()) {
                        if (vNode.getNodeStatus().equals(com.dss.backend.model.NodeStatus.ACTIVE)) {
                            double rand = Math.random() * 100;
                            if (rand < failurePercentage) {
                                String failedNodeId = vNode.getNodeId();
                                vNode.failNode();
                                eventLoggerService.logEvent(
                                        simulationId,
                                        "Node " + failedNodeId + " has failed automatically.",
                                        EventType.NODE_FAILED
                                );
                            }
                        }
                    }
                },
                intervalMillis,
                intervalMillis,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Allows an external caller to fail a specific node on demand,
     * simulating a user-induced failure.
     *
     * @param simulationId ID of the simulation
     * @param nodeId       ID of the node to fail
     */
    public void failNode(String simulationId, String nodeId) {
        VirtualNode vNode = nodeMap.get(nodeId);
        if (vNode != null) {
            vNode.failNode();
            eventLoggerService.logEvent(
                    simulationId,
                    "Node " + nodeId + " has been failed manually.",
                    EventType.NODE_FAILED
            );
        }
    }

    /**
     * Allows an external caller to recover a previously failed node on demand.
     * Mechanically this is just {@link VirtualNode#recoverNode()} -- what "catching
     * up" means afterward is protocol-specific (see {@code docs/consensus-architecture.md}).
     *
     * @param simulationId ID of the simulation
     * @param nodeId       ID of the node to recover
     */
    public void recoverNode(String simulationId, String nodeId) {
        VirtualNode vNode = nodeMap.get(nodeId);
        if (vNode != null) {
            vNode.recoverNode();
            eventLoggerService.logEvent(
                    simulationId,
                    "Node " + nodeId + " has been recovered manually.",
                    EventType.NODE_RECOVERED
            );
        }
    }

    /**
     * Broadcasts a proposal to every currently-active node, letting each node's
     * consensus algorithm decide whether to act on it (e.g. Raft followers ignore it;
     * only the leader appends and replicates). Callers don't need to know which node,
     * if any, is currently the leader.
     *
     * @param simulationId ID of the simulation
     * @param value        the value to propose
     */
    public void propose(String simulationId, Object value) {
        if (nodeMap == null) {
            return;
        }
        for (VirtualNode vNode : nodeMap.values()) {
            if (vNode.getNodeStatus() == NodeStatus.ACTIVE) {
                vNode.propose(value);
            }
        }
        eventLoggerService.logEvent(
                simulationId,
                "Value proposed: " + value,
                EventType.VALUE_PROPOSED
        );

        // For a leader-based protocol with no active leader, the proposal is silently
        // dropped (no node acts on it). Surface that instead of leaving the user to wonder
        // why nothing committed. This is the normal state for Multi-Paxos after its leader
        // fails (no re-election); for Raft it only happens in the brief window between a
        // leader failing and a new one being elected.
        boolean leaderBased = nodeMap.values().stream().anyMatch(vn -> isLeaderRole(vn.getRoleLabel()));
        boolean hasActiveLeader = nodeMap.values().stream()
                .anyMatch(vn -> vn.getNodeStatus() == NodeStatus.ACTIVE && "LEADER".equals(vn.getRoleLabel()));
        if (leaderBased && !hasActiveLeader) {
            eventLoggerService.logEvent(
                    simulationId,
                    "Proposal had no effect: no node is currently the leader.",
                    EventType.SIMULATION_EVENT
            );
        }
    }

    /** @return true if the role label belongs to a leader-election protocol (Raft, Multi-Paxos). */
    private static boolean isLeaderRole(String roleLabel) {
        return "LEADER".equals(roleLabel) || "FOLLOWER".equals(roleLabel) || "CANDIDATE".equals(roleLabel);
    }

    /**
     * Returns a live snapshot of every node's status and (protocol-specific) role, for
     * a dashboard's node grid. Unlike {@code GET /api/nodes}, which reads the static,
     * persisted node pool, this reflects this specific simulation's actual running
     * {@link VirtualNode} state.
     *
     * @return a list of {@link NodeDTO}s, or an empty list if this simulation hasn't
     *         initialized any nodes yet
     */
    public List<NodeDTO> getNodeStatuses() {
        if (nodeMap == null) {
            return Collections.emptyList();
        }
        List<NodeDTO> statuses = new ArrayList<>();
        for (VirtualNode vNode : nodeMap.values()) {
            NodeDTO dto = new NodeDTO();
            dto.setId(vNode.getNodeId());
            dto.setStatus(vNode.getNodeStatus().name());
            dto.setRoleLabel(vNode.getRoleLabel());
            Object committed = vNode.getCommittedValue();
            dto.setCommittedValue(committed == null ? null : String.valueOf(committed));
            dto.setDetail(vNode.getStateSummary());
            statuses.add(dto);
        }
        return statuses;
    }

    /**
     * Fetches the latest metrics from the {@link MetricsUpdateService}.
     * Typically, this is polled or displayed in the UI.
     *
     * @return the current metrics snapshot
     */
    public MetricsSnapshot getMetricsSnapshot() {
        return metricsUpdateService.getMetricsSnapshot();
    }

    /**
     * Stops the simulation by halting all virtual nodes (heartbeats, message loops)
     * and shutting down the {@link Scheduler}.
     *
     * @param simulationId the simulation ID
     */
    public void stopSimulation(String simulationId) {
        if (nodeMap != null) {
            for (VirtualNode vNode : nodeMap.values()) {
                vNode.stop();
            }
        }
        // Give a moment for tasks to gracefully shut down.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Shut down the central scheduler.
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }

        // Broadcast that the simulation has stopped.
        eventLoggerService.logEvent(simulationId, "Simulation stopped.", EventType.SIMULATION_EVENT);
    }
}