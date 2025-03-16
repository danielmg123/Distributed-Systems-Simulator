package com.dss.backend.engine;

import com.dss.backend.consensus.ConsensusAlgorithm;
import com.dss.backend.consensus.ConsensusAlgorithmFactory;
import com.dss.backend.failure.Heartbeat;
import com.dss.backend.failure.RingTopology;
import com.dss.backend.config.SimulationProperties;
import com.dss.backend.controller.SimulationWebSocketController;
import com.dss.backend.dto.EventDTO;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.VirtualNode;
import com.dss.backend.metrics.DefaultMetricsCollector;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.metrics.PerformanceMetricsCollector;
import com.dss.backend.model.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * <strong>SimulationEngine</strong> orchestrates the entire lifecycle of a simulation
 * from node initialization through runtime metrics gathering and eventual shutdown.
 * <p>
 * <strong>Lifecycle Overview:</strong>
 * <ol>
 *   <li><em>Initialization</em>: {@link #initializeNodes(List, SimulationConfig, TopologyType)}
 *       sets up the {@link VirtualNode} objects, their consensus algorithms, and heartbeats.</li>
 *   <li><em>Start</em>: {@link #startSimulation(String)} transitions the simulation to a running state:
 *       <ul>
 *         <li>Begins periodic metrics updates that send snapshots to the front-end.</li>
 *         <li>If a ring topology is used, starts periodic checks for node failures
 *             (using {@link RingTopology}).</li>
 *         <li>Sends a simulation event indicating that it has started.</li>
 *       </ul>
 *   </li>
 *   <li><em>Failure Simulation</em>: If configured, can randomly fail nodes at
 *       intervals (see {@link #startFailureSimulation(String, double, long)}),
 *       or a specific node can be failed via {@link #failNode(String, String)}.</li>
 *   <li><em>Stop</em>: {@link #stopSimulation(String)} halts the simulation by shutting
 *       down the {@link Scheduler} and each {@link VirtualNode} (thereby stopping heartbeats, etc.).
 *       Also sends a final simulation event indicating the stop.</li>
 * </ol>
 * <p>
 * <strong>Metrics and Failure Checks Scheduling:</strong>
 * <ul>
 *   <li>Metrics updates are scheduled via {@link #startMetricsUpdates(String)}, which uses
 *       {@link Scheduler#scheduleAtFixedRate(Runnable, long, long, TimeUnit)} to repeatedly push
 *       {@link MetricsSnapshot} data to the UI at a fixed interval.</li>
 *   <li>Failure checks (especially for a ring topology) are also scheduled with
 *       {@link #startRingFailureChecks()}, looking for overdue heartbeats in a ring arrangement.</li>
 * </ul>
 */
public class SimulationEngine {

    private final AppLogger appLogger = new DefaultAppLogger(SimulationEngine.class);

    // Map of nodeId -> VirtualNode
    private Map<String, VirtualNode> nodeMap = new ConcurrentHashMap<>();
    private MessageRouter messageRouter;
    private volatile boolean running = false;
    private RingTopology ringTopology;

    // Metrics Collector
    private final PerformanceMetricsCollector metricsCollector = new DefaultMetricsCollector();

    // WebSocket Controller for real-time updates
    private final SimulationWebSocketController simulationWebSocketController;

    private final Scheduler centralScheduler = new DefaultScheduler(Executors.newScheduledThreadPool(10));

    // Dedicated worker pool for processing VirtualNode messages
    private final ExecutorService workerPool = Executors.newFixedThreadPool(10);
    private final Random random = new Random();

    /**
     * Constructs the SimulationEngine with a dedicated {@link MessageRouter} and
     * a reference to a {@link SimulationWebSocketController} for pushing updates to clients.
     *
     * @param simulationWebSocketController used to send metrics and event updates via WebSockets
     */
    public SimulationEngine(SimulationWebSocketController simulationWebSocketController) {
        this.messageRouter = new MessageRouter();
        this.simulationWebSocketController = simulationWebSocketController;
    }

    /**
     * Initializes the nodes based on a list of {@link Node} objects and a simulation configuration.
     * <p>
     * Creates {@link VirtualNode} instances, each wrapping a {@link ConsensusAlgorithm}
     * (via {@link ConsensusAlgorithmFactory}), then registers them with the {@link MessageRouter}.
     * Also sets up heartbeats, scheduling them on the shared {@link Scheduler}.
     *
     * @param nodes        the list of node definitions
     * @param config       simulation config that includes consensus algorithm choice, etc.
     * @param topologyType determines how nodes might be arranged logically
     */
    public void initializeNodes(List<Node> nodes, SimulationConfig config, TopologyType topologyType) {
        List<String> allNodeIds = nodes.stream()
                .map(Node::getId)
                .collect(Collectors.toList());

        // Create a consensus factory
        SimulationProperties simulationProperties = new SimulationProperties();
        ConsensusAlgorithmFactory consensusFactory = new ConsensusAlgorithmFactory(messageRouter, centralScheduler, simulationProperties);

        for (Node node : nodes) {
            String nodeId = node.getId();
            ConsensusAlgorithm consensus = consensusFactory.createAlgorithm(nodeId, allNodeIds, config);
            VirtualNode vNode = new VirtualNode(node, consensus, messageRouter, workerPool, centralScheduler);

            // Start heartbeat
            Heartbeat heartbeat = new Heartbeat(messageRouter, nodeId, 1000L);
            vNode.setHeartbeat(heartbeat);
            heartbeat.start(centralScheduler);

            // Start the node
            vNode.start();

            messageRouter.registerNode(nodeId, vNode);
            nodeMap.put(nodeId, vNode);
        }

        // If ring topology is requested, instantiate the ring structure
        if (topologyType == TopologyType.RING) {
            ringTopology = new RingTopology(nodes);
        }
    }

    /**
     * Begins the simulation:
     * <ul>
     *   <li>Sets <code>running</code> to true.</li>
     *   <li>Schedules periodic metrics updates ({@link #startMetricsUpdates(String)}).</li>
     *   <li>If a ring topology is in use, calls {@link #startRingFailureChecks()}.</li>
     *   <li>Broadcasts a "Simulation started" event via WebSocket.</li>
     * </ul>
     *
     * @param simulationId the ID of the simulation used in WebSocket event messages
     */
    public void startSimulation(String simulationId) {
        running = true;
        startMetricsUpdates(simulationId);

        if (ringTopology != null) {
            startRingFailureChecks();
        }

        sendSimulationEvent(simulationId, "Simulation started.");
    }

    /**
     * Sets up periodic metrics updates. Uses the central {@link Scheduler} to
     * {@link Scheduler#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}
     * sending a {@link MetricsSnapshot} to the UI every 5 seconds.
     *
     * @param simulationId the simulation identifier for the WebSocket topic
     */
    public void startMetricsUpdates(String simulationId) {
        centralScheduler.scheduleAtFixedRate(() -> {
            if (running) {
                MetricsSnapshot snapshot = getMetricsSnapshot();
                simulationWebSocketController.sendMetricsUpdate(simulationId, snapshot);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    /**
     * If using a ring topology, sets up periodic checks for node failures by verifying
     * if successor nodes have missed their heartbeat threshold.
     * <p>
     * Scheduled via {@link Scheduler#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}.
     */
    public void startRingFailureChecks() {
        centralScheduler.scheduleAtFixedRate(() -> {
            for (Node node : ringTopology.getNodes()) {
                String failedSuccessor = ringTopology.checkSuccessorFailure(node.getId());
                if (failedSuccessor != null) {
                    appLogger.info("Node {} detected that its successor {} has failed.", node.getId(), failedSuccessor);
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    /**
     * Sends an event notification via WebSocket to indicate simulation activity (start, stop, node fail, etc.).
     *
     * @param simulationId the simulation ID for the WebSocket topic
     * @param message      the text of the event
     */
    public void sendSimulationEvent(String simulationId, String message) {
        EventDTO event = new EventDTO();
        event.setType(EventType.SIMULATION_EVENT);
        event.setDetails(message);
        event.setTimestamp(LocalDateTime.now());
        simulationWebSocketController.sendEventUpdate(simulationId, event);
    }

    /**
     * Begins a periodic task that randomly fails active nodes with a given probability
     * expressed as a percentage. This can be used to simulate random node crashes during the simulation.
     *
     * @param simulationId       the simulation ID, for event logging
     * @param failurePercentage  chance (in %) that a given node will fail at each interval
     * @param intervalMillis     how often (in milliseconds) to attempt failing nodes
     */
    public void startFailureSimulation(String simulationId, double failurePercentage, long intervalMillis) {
        centralScheduler.scheduleAtFixedRate(() -> {
            for (VirtualNode vNode : nodeMap.values()) {
                if (vNode.getNodeStatus() == NodeStatus.ACTIVE && random.nextDouble() < (failurePercentage / 100.0)) {
                    String failedNodeId = vNode.getNodeId();
                    vNode.failNode();
                    sendSimulationEvent(simulationId, "Node " + failedNodeId + " has failed.");
                }
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops the simulation by stopping all VirtualNodes, shutting down the scheduler,
     * and sending a "Simulation stopped" event.
     *
     * @param simulationId the ID used for broadcasting stop event
     */
    public void stopSimulation(String simulationId) {
        running = false;
        for (VirtualNode vNode : nodeMap.values()) {
            vNode.stop();
        }
        centralScheduler.shutdown();
        try {
            if (!centralScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                centralScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            centralScheduler.shutdownNow();
        }
        sendSimulationEvent(simulationId, "Simulation stopped.");
    }

    /**
     * Explicitly fails a specified node, simulating a crash.
     *
     * @param simulationId the simulation ID for logging
     * @param nodeId       which node to fail
     */
    public void failNode(String simulationId, String nodeId) {
        VirtualNode vNode = nodeMap.get(nodeId);
        if (vNode != null) {
            vNode.failNode();
            sendSimulationEvent(simulationId, "Node " + nodeId + " has failed.");
        }
    }

    /**
     * Gathers the current performance metrics from the shared {@link PerformanceMetricsCollector}.
     *
     * @return a snapshot of metrics (e.g., message count, latencies)
     */
    public MetricsSnapshot getMetricsSnapshot() {
        return metricsCollector.getSnapshot();
    }
}