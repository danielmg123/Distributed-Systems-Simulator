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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class SimulationEngine {

    private static final Logger logger = LoggerFactory.getLogger(SimulationEngine.class);

    // Map of nodeId -> VirtualNode
    private Map<String, VirtualNode> nodeMap = new ConcurrentHashMap<>();
    private MessageRouter messageRouter;
    private volatile boolean running = false;
    private RingTopology ringTopology;

    // Metrics Collector
    private final PerformanceMetricsCollector metricsCollector = new DefaultMetricsCollector();

    // WebSocket Controller for real-time updates
    private final SimulationWebSocketController simulationWebSocketController;

    // Centralized scheduler for simulation and per-node tasks
    private final ScheduledExecutorService centralScheduler = Executors.newScheduledThreadPool(10);
    // Dedicated worker pool for processing VirtualNode messages
    private final ExecutorService workerPool = Executors.newFixedThreadPool(10);
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
     * @param nodes        List of nodes participating in the simulation.
     * @param config       Simulation configuration
     * @param topologyType Type of topology used
     */
    public void initializeNodes(List<Node> nodes, SimulationConfig config, TopologyType topologyType) {
        List<String> allNodeIds = nodes.stream()
                .map(Node::getId)
                .collect(Collectors.toList());

        // Create a ConsensusAlgorithmFactory with the shared messageRouter and scheduler.
        ConsensusAlgorithmFactory consensusFactory = new ConsensusAlgorithmFactory(messageRouter, centralScheduler);

        for (Node node : nodes) {
            String nodeId = node.getId();
            ConsensusAlgorithm consensus = consensusFactory.createAlgorithm(nodeId, allNodeIds, config);
            // Create VirtualNode with the injected workerPool and scheduler.
            VirtualNode vNode = new VirtualNode(node, consensus, messageRouter, workerPool, centralScheduler);

            // Setup and start heartbeat.
            Heartbeat heartbeat = new Heartbeat(messageRouter, nodeId);
            vNode.setHeartbeat(heartbeat);
            heartbeat.start(centralScheduler);

            // Start the VirtualNode processing (message loop, phi-checker).
            vNode.start();

            messageRouter.registerNode(nodeId, vNode);
            nodeMap.put(nodeId, vNode);
        }

        if (topologyType == TopologyType.RING) {
            ringTopology = new RingTopology(nodes);
        }
    }

    /**
     * Starts the simulation.
     */
    public void startSimulation(String simulationId) {
        running = true;
        // Nodes were already started during initialization.
        startMetricsUpdates(simulationId);

        if (ringTopology != null) {
            startRingFailureChecks();
        }

        sendSimulationEvent(simulationId, "Simulation started.");
    }

    /**
     * Schedules periodic metrics updates.
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
                    logger.info("Node {} detected that its successor {} has failed.", node.getId(), failedSuccessor);
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    /**
     * Sends a simulation event via WebSocket.
     */
    public void sendSimulationEvent(String simulationId, String message) {
        EventDTO event = new EventDTO();
        event.setType(EventType.SIMULATION_EVENT);
        event.setDetails(message);
        event.setTimestamp(LocalDateTime.now());
        simulationWebSocketController.sendEventUpdate(simulationId, event);
    }

    /**
     * Starts failure simulation.
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
     * Stops the simulation.
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
     * Simulates failure of a specific node.
     */
    public void failNode(String simulationId, String nodeId) {
        VirtualNode vNode = nodeMap.get(nodeId);
        if (vNode != null) {
            vNode.failNode();
            sendSimulationEvent(simulationId, "Node " + nodeId + " has failed.");
        }
    }

    /**
     * Retrieves the current metrics snapshot.
     */
    public MetricsSnapshot getMetricsSnapshot() {
        return metricsCollector.getSnapshot();
    }
}