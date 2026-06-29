package com.dss.backend.service;

import com.dss.backend.controller.SimulationWebSocketController;
import com.dss.backend.dto.NodeDTO;
import com.dss.backend.engine.DefaultScheduler;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.metrics.PerformanceMetricsCollector;
import com.dss.backend.metrics.DefaultMetricsCollector;
import com.dss.backend.model.*;
import com.dss.backend.repository.SimulationRepository;
import com.dss.backend.service.engine.*;
import com.dss.backend.consensus.ConsensusAlgorithmFactory;
import com.dss.backend.consensus.ConsensusObserver;
import com.dss.backend.config.SimulationProperties;

import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * <p>
 * The {@code SimulationService} provides high-level orchestration for creating,
 * running, and stopping simulations, as well as retrieving simulation metrics.
 * </p>
 *
 * <p>
 * <strong>Business Logic & Responsibilities:</strong>
 * <ul>
 *   <li>Manages {@code Simulation} objects (CRUD) in the database.</li>
 *   <li>Coordinates the start and stop of simulations by delegating to a
 *       {@link SimulationOrchestrator} object.</li>
 *   <li>Handles node failures in the context of a running simulation.</li>
 *   <li>Provides aggregated performance metrics for each simulation.</li>
 * </ul>
 * </p>
 *
 * <p>
 * <strong>Side Effects & Detailed Behavior:</strong>
 * <ul>
 *   <li><strong>Orchestrator Management:</strong>
 *       The service keeps an in-memory map of simulation IDs to their active orchestrators.
 *       When a simulation is run, an orchestrator is created, started, and stored in this map.
 *       When a simulation is stopped, the orchestrator is removed, and the resources
 *       are freed/shutdown.</li>
 *   <li><strong>Event Logging:</strong>
 *       This service optionally logs events (e.g., "Simulation started") to
 *       the front end or a database. The event logging is typically handled by
 *       an {@link EventLoggerService}, which is used inside the orchestrator classes.</li>
 *   <li><strong>Metrics Collector:</strong>
 *       We keep a shared {@link PerformanceMetricsCollector} that accumulates
 *       data about message latencies, proposals, commits, etc. The service can query
 *       these metrics via {@link #getSimulationMetrics(String)} to present a snapshot
 *       to the UI.</li>
 * </ul>
 * </p>
 */
@Service
public class SimulationService {

    private final AppLogger appLogger = new DefaultAppLogger(SimulationService.class);

    private final SimulationRepository simulationRepository;
    private final SimulationWebSocketController simulationWebSocketController;

    /**
     * Global simulation properties that define thread pool sizes, timeouts, etc.
     */
    private final SimulationProperties simulationProperties;

    /**
     * In-memory mapping of simulation IDs to their active orchestrators.
     * Once a simulation is started, an {@link SimulationOrchestrator} is created and stored here.
     * When the simulation is stopped, the orchestrator is removed.
     */
    private final Map<String, SimulationOrchestrator> orchestrators = new ConcurrentHashMap<>();

    /**
     * A shared performance metrics collector to accumulate stats across simulations.
     */
    private final PerformanceMetricsCollector metricsCollector = new DefaultMetricsCollector();

    public SimulationService(SimulationRepository simulationRepository,
                             SimulationWebSocketController simulationWebSocketController,
                             SimulationProperties simulationProperties) {
        this.simulationRepository = simulationRepository;
        this.simulationWebSocketController = simulationWebSocketController;
        this.simulationProperties = simulationProperties;
    }

    /**
     * <p>Retrieves all simulations stored in the database.</p>
     *
     * @return list of all {@link Simulation} entities.
     */
    public List<Simulation> getAllSimulations() {
        return simulationRepository.findAll();
    }

    /**
     * <p>Locates a single simulation by ID, or throws an exception if not found.</p>
     *
     * @param id the unique database ID of the simulation.
     * @return the retrieved {@link Simulation}.
     * @throws ResourceNotFoundException if the simulation does not exist.
     */
    public Simulation getSimulationByIdOrThrow(String id) {
        return simulationRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + id));
    }

    /**
     * <p>Saves a new or existing simulation record in the database.</p>
     *
     * @param simulation the simulation data to be persisted.
     * @return the saved {@link Simulation}, including a database-assigned ID if it was new.
     */
    public Simulation saveSimulation(Simulation simulation) {
        return simulationRepository.save(simulation);
    }

    /**
     * <p>Deletes a simulation record from the database, if found. Otherwise, throws an exception.</p>
     *
     * @param id the ID of the simulation to remove.
     * @throws ResourceNotFoundException if the simulation does not exist.
     */
    public void deleteSimulation(String id) {
        simulationRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + id));
        simulationRepository.deleteById(id);
    }

    /**
     * <p>Starts (runs) a simulation:
     * <ol>
     *   <li>Find the simulation by ID in the database.</li>
     *   <li>Retrieve all {@link Node} objects to use in that simulation.</li>
     *   <li>Create a new {@link SimulationOrchestrator} and register it in the {@link #orchestrators} map.</li>
     *   <li>Initialize the orchestrator with the node list and the simulation config.</li>
     *   <li>Mark the simulation's status as RUNNING and save it back to the database.</li>
     *   <li>Finally, the orchestrator is told to actually start (begin heartbeats, failure checks, etc.).</li>
     *   <li>If a failure percentage is given, sets up failure simulation logic as well.</li>
     * </ol>
     * </p>
     *
     * @param simulationId the unique ID of the simulation to run.
     * @throws ResourceNotFoundException if the simulation does not exist.
     */
    public void runSimulation(String simulationId) {
        // 1. Retrieve the simulation from the database
        Simulation simulation = simulationRepository.findById(simulationId)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + simulationId));

        // 2. Build this simulation's node set from its configured node count. Nodes are
        //    ephemeral to the run -- they are NOT read from the persisted node pool, so
        //    two simulations can't share or clobber each other's nodes and a leftover
        //    node from an earlier run can't silently join this one.
        SimulationConfig config = simulation.getConfig();
        int nodeCount = (config != null && config.getNodeCount() > 0) ? config.getNodeCount() : 3;
        List<Node> nodes = new ArrayList<>();
        for (int i = 1; i <= nodeCount; i++) {
            Node node = new Node();
            node.setId("node" + i);
            node.setAddress("10.0.0." + i);
            node.setStatus(NodeStatus.ACTIVE);
            nodes.add(node);
        }

        // 3. Set up a fresh, per-simulation scheduler plus the router, consensus factory,
        // and supporting services for the orchestrator.
        //
        // The scheduler is created per run (not shared/singleton) on purpose: stopping a
        // simulation shuts its scheduler down (see SimulationOrchestrator.stopSimulation),
        // so a shared scheduler would be left permanently terminated and every later
        // simulation would silently lose its heartbeats, Raft election timers, delayed
        // delivery, and metrics pushes. A per-run scheduler scopes that shutdown to the
        // one simulation being stopped and lets independent simulations run concurrently.
        //
        // The router shares this service's metricsCollector so dropped-message counts
        // (e.g. messages routed to a FAILED node, or randomly lost) show up in
        // getSimulationMetrics(). It also shares this run's scheduler so configured message
        // delay can actually be simulated (see setMessageLossRate/setMaxDelayMs below).
        Scheduler scheduler = new DefaultScheduler(
                Executors.newScheduledThreadPool(simulationProperties.getSchedulerThreadPoolSize()));
        MessageRouter router = new MessageRouter(metricsCollector, scheduler);
        if (config != null) {
            router.setMessageLossRate(config.getMessageLossRate());
            router.setMinDelayMs(config.getMinMessageDelayMs());
            router.setMaxDelayMs(config.getMaxMessageDelayMs());
        }
        EventLoggerService eventLoggerService = new EventLoggerService(simulationWebSocketController);
        // Each algorithm reports notable moments through this observer: a commit (counted
        // once per cluster-agreed value, and logged to the event feed) and, for Raft, a
        // leader election. This is what surfaces "node3 became leader" / "x=1 committed"
        // in the dashboard's event log.
        ConsensusObserver observer = new ConsensusObserver() {
            @Override
            public void onCommitted(String nodeId, Object value) {
                metricsCollector.recordCommit();
                eventLoggerService.logEvent(simulationId,
                        "Node " + nodeId + " committed value: " + value, EventType.VALUE_COMMITTED);
            }

            @Override
            public void onLeaderElected(String nodeId, int term) {
                eventLoggerService.logEvent(simulationId,
                        "Node " + nodeId + " became leader (term " + term + ")", EventType.LEADER_ELECTED);
            }
        };
        // Scale Raft's election timeout up with the configured network delay. With the
        // default 150-300ms range, once delivery delay approaches ~150ms a leader's
        // heartbeats can land after a follower's timeout, triggering endless re-elections.
        // Keeping the timeout comfortably above the max delay (and never below the
        // configured floor) lets a healthy leader hold the role.
        long maxDelayMs = (config != null) ? config.getMaxMessageDelayMs() : 0;
        long electionTimeoutMin = Math.max(simulationProperties.getRaftElectionTimeoutMinMillis(), 3 * maxDelayMs);
        long electionTimeoutMax = Math.max(simulationProperties.getRaftElectionTimeoutMaxMillis(), 6 * maxDelayMs);
        ConsensusAlgorithmFactory consensusFactory = new ConsensusAlgorithmFactory(
                router, scheduler, simulationProperties, observer, electionTimeoutMin, electionTimeoutMax);
        NodeInitializationService nodeInitService = new NodeInitializationService(router, scheduler, consensusFactory, simulationProperties);
        MetricsUpdateService metricsUpdateService = new MetricsUpdateService(metricsCollector, simulationWebSocketController, scheduler);

        SimulationOrchestrator orchestrator = new SimulationOrchestrator(
                router,
                scheduler,
                nodeInitService,
                metricsUpdateService,
                eventLoggerService
        );

        // 4. Initialize nodes in the orchestrator
        orchestrator.initializeSimulationNodes(nodes, simulation.getConfig(), simulation.getConfig().getTopologyType());

        // 5. Store the orchestrator in our map for future reference (failNode, stop, etc.)
        orchestrators.put(simulationId, orchestrator);

        // 6. Update the simulation status to RUNNING and save
        simulation.setStatus(SimulationStatus.RUNNING);
        simulationRepository.save(simulation);

        // 7. Start the simulation orchestration
        orchestrator.startSimulation(simulationId);

        // 8. If a failure percentage is configured, start a failure simulation
        if (simulation.getConfig() != null) {
            double failurePercentage = simulation.getConfig().getFailurePercentage();
            if (failurePercentage > 0) {
                // e.g., every 5000 ms, random nodes may fail
                orchestrator.startFailureSimulation(simulationId, failurePercentage, 5000);
                appLogger.info("Started failure simulation with {}% failure rate.", failurePercentage);

                // Event logs (the orchestrator also logs events, so we could do it here or in orchestrator)
            }
        }
    }

    /**
     * <p>Stops (ends) the simulation and removes its orchestrator from memory.
     * <ol>
     *   <li>Look up the orchestrator in the map by {@code simulationId}.</li>
     *   <li>Invoke {@code stopSimulation(...)} on it, which halts nodes, heartbeats, etc.</li>
     *   <li>Remove it from the map to free resources.</li>
     *   <li>Update the simulation status to COMPLETED and save to the database.</li>
     * </ol>
     * </p>
     *
     * @param simulationId the ID of the simulation to stop.
     */
    public void stopSimulation(String simulationId) {
        // Retrieve the orchestrator if it exists
        SimulationOrchestrator orchestrator = orchestrators.get(simulationId);
        if (orchestrator != null) {
            // Stop all node processes, heartbeats, scheduled tasks, etc.
            orchestrator.stopSimulation(simulationId);
            // Remove from map
            orchestrators.remove(simulationId);

            // Mark simulation as COMPLETED in the database
            Simulation simulation = simulationRepository.findById(simulationId)
                    .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + simulationId));
            simulation.setStatus(SimulationStatus.COMPLETED);
            simulationRepository.save(simulation);
        }
    }

    /**
     * <p>Fails a specific node within a simulation, typically used
     * to simulate node crashes on demand.</p>
     *
     * <p>This updates the node's status in memory (orchestrator) to FAILED
     * and triggers any event logging or related side effects in the orchestrator.
     * It does not necessarily remove the node from the DB, but marks it as failed
     * for the simulation run.</p>
     *
     * @param simulationId the ID of the running simulation.
     * @param nodeId the node to be forcibly failed.
     */
    public void failNode(String simulationId, String nodeId) {
        SimulationOrchestrator orchestrator = orchestrators.get(simulationId);
        if (orchestrator != null) {
            orchestrator.failNode(simulationId, nodeId);
        }
        // If the simulation orchestrator does not exist (simulation not running),
        // we do nothing or could log a warning.
    }

    /**
     * <p>Recovers a previously failed node within a running simulation. A no-op if the
     * simulation isn't currently running.</p>
     *
     * @param simulationId the ID of the running simulation.
     * @param nodeId the node to recover.
     */
    public void recoverNode(String simulationId, String nodeId) {
        SimulationOrchestrator orchestrator = orchestrators.get(simulationId);
        if (orchestrator != null) {
            orchestrator.recoverNode(simulationId, nodeId);
        }
    }

    /**
     * <p>Broadcasts a proposal to every active node in a running simulation. Each node's
     * consensus algorithm decides whether to act on it (e.g. only a Raft leader does).
     * A no-op if the simulation isn't currently running.</p>
     *
     * @param simulationId the ID of the running simulation.
     * @param value the value to propose.
     */
    public void propose(String simulationId, Object value) {
        SimulationOrchestrator orchestrator = orchestrators.get(simulationId);
        if (orchestrator != null) {
            metricsCollector.recordProposal();
            orchestrator.propose(simulationId, value);
        }
    }

    /**
     * <p>Returns a live snapshot of every node's status and (protocol-specific) role for
     * a running simulation, for a dashboard's node grid. Returns an empty list if the
     * simulation isn't currently running.</p>
     *
     * @param simulationId the ID of the running simulation.
     */
    public List<NodeDTO> getNodeStatuses(String simulationId) {
        SimulationOrchestrator orchestrator = orchestrators.get(simulationId);
        if (orchestrator == null) {
            return List.of();
        }
        return orchestrator.getNodeStatuses();
    }

    /**
     * <p>Retrieves performance metrics for the specified simulation, such as message counts,
     * latencies, proposals, and commits.</p>
     *
     * <p>
     * If the simulation is running, we fetch the snapshot from its orchestrator's
     * {@link MetricsUpdateService}. Otherwise, we return a snapshot from the shared
     * collector (which may be the default or last known data if the simulation is stopped).
     * </p>
     *
     * @param simulationId the unique ID of the simulation.
     * @return a snapshot of metrics data relevant to that simulation.
     */
    public MetricsSnapshot getSimulationMetrics(String simulationId) {
        // Attempt to retrieve orchestrator for the given simulation
        SimulationOrchestrator orchestrator = orchestrators.get(simulationId);
        if (orchestrator == null) {
            // e.g., simulation might be stopped. We fall back to the shared collector's snapshot
            return metricsCollector.getSnapshot();
        }
        // If running, orchestrator returns real-time data
        return orchestrator.getMetricsSnapshot();
    }

    /**
     * <p>Retrieves the neighbor-adjacency map computed for the simulation's chosen
     * topology, for a dashboard to render. This is visualization metadata only --
     * see {@link com.dss.backend.messaging.TopologyPlacer} for why it does not affect
     * how messages are actually routed between nodes.</p>
     *
     * @param simulationId the unique ID of the simulation.
     * @return a map of node ID -> list of neighbor node IDs, or an empty map if the
     *         simulation isn't currently running or no topology was configured.
     */
    public Map<String, List<String>> getTopologyMapping(String simulationId) {
        SimulationOrchestrator orchestrator = orchestrators.get(simulationId);
        if (orchestrator == null) {
            return Map.of();
        }
        return orchestrator.getTopologyMapping();
    }

    /**
     * <p>Live-updates the simulated network conditions (random message loss and
     * delivery delay) for a running simulation, taking effect immediately for all
     * subsequent message sends. Intended to back UI controls (e.g. sliders) that let a
     * user tune conditions without restarting the simulation.</p>
     *
     * <p>If the simulation isn't currently running (no orchestrator registered), this
     * is a no-op -- there's no live {@link com.dss.backend.messaging.MessageRouter} to
     * update.</p>
     *
     * @param simulationId    the unique ID of the simulation.
     * @param messageLossRate probability (0.0-1.0) that a message is randomly dropped.
     * @param minDelayMs      minimum simulated delivery delay, in milliseconds.
     * @param maxDelayMs      maximum simulated delivery delay, in milliseconds (0 disables delay).
     */
    public void updateNetworkConditions(String simulationId, double messageLossRate, long minDelayMs, long maxDelayMs) {
        SimulationOrchestrator orchestrator = orchestrators.get(simulationId);
        if (orchestrator != null) {
            orchestrator.setNetworkConditions(messageLossRate, minDelayMs, maxDelayMs);
        }
    }
}