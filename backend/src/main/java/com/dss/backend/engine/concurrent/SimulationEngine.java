package com.dss.backend.engine.concurrent;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.ConsensusAlgorithmFactory;
import com.dss.backend.algorithm.failure.Heartbeat;
import com.dss.backend.algorithm.failure.RingTopology;
import com.dss.backend.controller.SimulationWebSocketController;
import com.dss.backend.dto.EventDTO;
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

public class SimulationEngine {

    // A map of nodeId -> VirtualNodeThread
    private Map<String, VirtualNodeThread> nodeThreads = new ConcurrentHashMap<>();
    private MessageRouter messageRouter;
    private volatile boolean running = false;
    private RingTopology ringTopology;

    // Metrics Collector
    private final PerformanceMetricsCollector metricsCollector = new DefaultMetricsCollector();

    // WebSocket Controller for real-time updates
    private final SimulationWebSocketController simulationWebSocketController;

    // Centralized scheduler for all simulation tasks and per-node tasks
    private final ScheduledExecutorService centralScheduler = Executors.newScheduledThreadPool(10);
    private final Random random = new Random();

    /**
     * Constructor for SimulationEngine.
     *
     * @param simulationWebSocketController WebSocket controller for real-time updates
     */
    public SimulationEngine(SimulationWebSocketController simulationWebSocketController) {
        this.messageRouter = new MessageRouter();
        this.simulationWebSocketController = simulationWebSocketController;
    }

    /**
     * Initializes nodes for the simulation.
     *
     * @param nodes     List of nodes participating in the simulation.
     * @param config    Simulation configurations
     * @param topologyType type of topology used
     */

    public void initializeNodes(List<Node> nodes, SimulationConfig config, TopologyType topologyType) {
        // Get the list of all node IDs from the nodes list.
        List<String> allNodeIds = nodes.stream()
                .map(Node::getId)
                .collect(Collectors.toList());

        // For each node, create its own consensus algorithm instance using its unique id.
        for (Node node : nodes) {
            String nodeId = node.getId();
            ConsensusAlgorithm consensus = ConsensusAlgorithmFactory.createAlgorithm(
                    nodeId,         // Use the node's own id.
                    allNodeIds,     // List of all node IDs.
                    config,         // Simulation configuration.
                    messageRouter   // Shared message router.
            );

            // Create the VirtualNodeThread for this node with its unique consensus instance.
            VirtualNodeThread vThread = new VirtualNodeThread(node, consensus, messageRouter);

            // Create and start the heartbeat using the central scheduler.
            Heartbeat heartbeat = new Heartbeat(messageRouter, nodeId);
            vThread.setHeartbeat(heartbeat);
            heartbeat.startHeartbeat(centralScheduler);

            // Start the phi-checker using the central scheduler.
            vThread.startPhiChecker(centralScheduler);

            messageRouter.registerNode(nodeId, vThread);
            nodeThreads.put(nodeId, vThread);
        }

        // If a ring topology is used, initialize it.
        if (topologyType == TopologyType.RING) {
            ringTopology = new RingTopology(nodes);
        }
    }

    /**
     * Starts the simulation by running node threads.
     */
    public void startSimulation(String simulationId) {
        running = true;

        // Start each node thread
        for (VirtualNodeThread vThread : nodeThreads.values()) {
            vThread.start();
        }

        // Start periodic metrics updates
        startMetricsUpdates(simulationId);

        if (ringTopology != null) {
            startRingFailureChecks();
        }

        // Notify clients that simulation has started
        sendSimulationEvent(simulationId, "Simulation started.");
    }

    /**
     * Starts a periodic task to push metrics updates to WebSocket clients.
     *
     * @param simulationId The simulation identifier.
     */
    public void startMetricsUpdates(String simulationId) {
        centralScheduler.scheduleAtFixedRate(() -> {
            if (running) {
                MetricsSnapshot snapshot = getMetricsSnapshot();
                simulationWebSocketController.sendMetricsUpdate(simulationId, snapshot);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    public void startRingFailureChecks() {
        centralScheduler.scheduleAtFixedRate(() -> {
            for (Node node : ringTopology.getNodes()) {
                String failedSuccessor = ringTopology.checkSuccessorFailure(node.getId());
                if (failedSuccessor != null) {
                    System.out.println("Node " + node.getId() + " detected that its successor "
                            + failedSuccessor + " has failed.");
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    /**
     * Sends a WebSocket event update.
     *
     * @param simulationId Simulation identifier.
     * @param message      Event message.
     */
    public void sendSimulationEvent(String simulationId, String message) {
        EventDTO event = new EventDTO();
        event.setType(EventType.SIMULATION_EVENT);
        event.setDetails(message);
        event.setTimestamp(LocalDateTime.now());

        simulationWebSocketController.sendEventUpdate(simulationId, event);
    }

    /**
     * Starts a periodic task that randomly fails nodes based on a failure percentage.
     *
     * @param failurePercentage The percentage of nodes to fail (e.g., 10 for 10%).
     * @param intervalMillis    How frequently (in milliseconds) to trigger the failure check.
     */
    public void startFailureSimulation(String simulationId, double failurePercentage, long intervalMillis) {
        centralScheduler.scheduleAtFixedRate(() -> {
            for (VirtualNodeThread vThread : nodeThreads.values()) {
                if (vThread.getNodeStatus() == NodeStatus.ACTIVE && random.nextDouble() < (failurePercentage / 100.0)) {
                    String failedNodeId = vThread.getNodeId();
                    vThread.failNode();
                    sendSimulationEvent(simulationId, "Node " + failedNodeId + " has failed.");
                }
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops the simulation, stopping all node threads and cleanup.
     */
    public void stopSimulation(String simulationId) {
        running = false;
        for (VirtualNodeThread vThread : nodeThreads.values()) {
            vThread.requestStop();
            // Cancel per‑node scheduled tasks.
            vThread.stopPhiChecker();
            if (vThread.getHeartbeat() != null) {
                vThread.getHeartbeat().stopHeartbeat();
            }
        }
        // Shut down the central scheduler cleanly.
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
     * Simulates the failure of a specific node.
     *
     * @param simulationId Simulation identifier.
     * @param nodeId       ID of the node to fail.
     */
    public void failNode(String simulationId, String nodeId) {
        VirtualNodeThread vThread = nodeThreads.get(nodeId);
        if (vThread != null) {
            vThread.failNode();

            // Send failure event update
            sendSimulationEvent(simulationId, "Node " + nodeId + " has failed.");
        }
    }

    /**
     * Retrieves the current metrics snapshot.
     *
     * @return MetricsSnapshot containing simulation performance data.
     */
    public MetricsSnapshot getMetricsSnapshot() {
        return metricsCollector.getSnapshot();
    }
}
