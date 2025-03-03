package com.dss.backend.engine.service;

import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.VirtualNode;
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

public class SimulationOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(SimulationOrchestrator.class);

    private final MessageRouter messageRouter;
    private final ScheduledExecutorService scheduler;
    private final NodeInitializationService nodeInitializationService;
    private final MetricsUpdateService metricsUpdateService;
    private final EventLoggerService eventLoggerService;

    // Map of nodeId -> VirtualNode
    private Map<String, VirtualNode> nodeMap;
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
        nodeMap = nodeInitializationService.initializeNodes(nodes, config, topologyType);
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
     * Starts the simulation by starting metrics updates and logging the simulation start.
     * (VirtualNodes were already started during initialization.)
     */
    public void startSimulation(String simulationId) {
        metricsUpdateService.startMetricsUpdates(simulationId);
        eventLoggerService.logEvent(simulationId, "Simulation started.", EventType.SIMULATION_STARTED);
    }

    /**
     * Starts a periodic task that randomly fails active nodes based on a given failure percentage.
     *
     * @param simulationId     Simulation identifier.
     * @param failurePercentage Percentage of nodes to fail.
     * @param intervalMillis   Interval between failure checks.
     */
    public void startFailureSimulation(String simulationId, double failurePercentage, int intervalMillis) {
        scheduler.scheduleAtFixedRate(() -> {
            for (VirtualNode vNode : nodeMap.values()) {
                if (vNode.getNodeStatus().equals(com.dss.backend.model.NodeStatus.ACTIVE)) {
                    double rand = Math.random() * 100;
                    if (rand < failurePercentage) {
                        String failedNodeId = vNode.getNodeId();
                        vNode.failNode();
                        eventLoggerService.logEvent(simulationId, "Node " + failedNodeId + " has failed automatically.", EventType.NODE_FAILED);
                    }
                }
            }
        }, intervalMillis, intervalMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * Fails a specific node.
     *
     * @param simulationId Simulation identifier.
     * @param nodeId       ID of the node to fail.
     */
    public void failNode(String simulationId, String nodeId) {
        VirtualNode vNode = nodeMap.get(nodeId);
        if (vNode != null) {
            vNode.failNode();
            eventLoggerService.logEvent(simulationId, "Node " + nodeId + " has been failed manually.", EventType.NODE_FAILED);
        }
    }

    /**
     * Retrieves the current metrics snapshot.
     *
     * @return MetricsSnapshot containing current simulation performance data.
     */
    public MetricsSnapshot getMetricsSnapshot() {
        return metricsUpdateService.getMetricsSnapshot();
    }

    /**
     * Stops the simulation by stopping all VirtualNodes and shutting down the scheduler.
     *
     * @param simulationId Simulation identifier.
     */
    public void stopSimulation(String simulationId) {
        if (nodeMap != null) {
            for (VirtualNode vNode : nodeMap.values()) {
                vNode.stop();
            }
        }
        // Shut down the scheduler cleanly.
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        eventLoggerService.logEvent(simulationId, "Simulation stopped.", EventType.SIMULATION_EVENT);
    }
}