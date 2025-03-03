package com.dss.backend.engine.service;

import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.VirtualNodeThread;
import com.dss.backend.engine.concurrent.TopologyPlacer;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.model.EventType;
import com.dss.backend.model.Node;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.model.TopologyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SimulationOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(SimulationOrchestrator.class);

    private final MessageRouter messageRouter;
    private final ScheduledExecutorService scheduler;
    private final NodeInitializationService nodeInitializationService;
    private final MetricsUpdateService metricsUpdateService;
    private final EventLoggerService eventLoggerService;

    private Map<String, VirtualNodeThread> nodeThreads;
    private Map<String, List<String>> topologyMapping;

    public SimulationOrchestrator(MessageRouter messageRouter,
                                  ScheduledExecutorService scheduler,
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
     * Initializes the simulation nodes and computes the topology (if applicable).
     */
    public void initializeSimulationNodes(List<Node> nodes, SimulationConfig config, TopologyType topologyType) {
        nodeThreads = nodeInitializationService.initializeNodes(nodes, config, topologyType);
        if (topologyType != null) {
            topologyMapping = TopologyPlacer.assignNeighbors(topologyType, nodes);
            logger.info("Topology mapping: {}", topologyMapping);
        }
    }

    /**
     * Computes the topology mapping based on the provided nodes and topology type.
     */
    public Map<String, List<String>> computeTopologyMapping(List<Node> nodes, TopologyType topologyType) {
        return TopologyPlacer.assignNeighbors(topologyType, nodes);
    }

    /**
     * Starts the simulation by starting all node threads and periodic metrics updates.
     */
    public void startSimulation(String simulationId) {
        if (nodeThreads != null) {
            for (VirtualNodeThread vThread : nodeThreads.values()) {
                vThread.start();
            }
        }
        metricsUpdateService.startMetricsUpdates(simulationId);
        eventLoggerService.logEvent(simulationId, "Simulation started.", EventType.SIMULATION_STARTED);
    }

    /**
     * Starts failure simulation by scheduling a task that randomly fails active nodes.
     */
    public void startFailureSimulation(String simulationId, double failurePercentage, int intervalMillis) {
        scheduler.scheduleAtFixedRate(() -> {
            for (VirtualNodeThread vThread : nodeThreads.values()) {
                if (vThread.getNodeStatus().equals(com.dss.backend.model.NodeStatus.ACTIVE)) {
                    double rand = Math.random() * 100;
                    if (rand < failurePercentage) {
                        vThread.failNode();
                        eventLoggerService.logEvent(simulationId, "Node " + vThread.getNodeId() + " has failed automatically.", EventType.NODE_FAILED);
                    }
                }
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Fails a specific node.
     */
    public void failNode(String simulationId, String nodeId) {
        VirtualNodeThread vThread = nodeThreads.get(nodeId);
        if (vThread != null) {
            vThread.failNode();
            eventLoggerService.logEvent(simulationId, "Node " + nodeId + " has been failed manually.", EventType.NODE_FAILED);
        }
    }

    /**
     * Returns the current metrics snapshot.
     */
    public MetricsSnapshot getMetricsSnapshot() {
        return metricsUpdateService.getMetricsSnapshot();
    }

    /**
     * Stops the simulation by stopping all node threads and shutting down the scheduler.
     */
    public void stopSimulation(String simulationId) {
        if (nodeThreads != null) {
            for (VirtualNodeThread vThread : nodeThreads.values()) {
                vThread.requestStop();
                vThread.stopPhiChecker();
                if (vThread.getHeartbeat() != null) {
                    vThread.getHeartbeat().stop();
                }
            }
        }
        // Note: If you have a dedicated scheduler per simulation, you might not want to shut it down here.
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        eventLoggerService.logEvent(simulationId, "Simulation stopped.", EventType.SIMULATION_EVENT);
    }
}