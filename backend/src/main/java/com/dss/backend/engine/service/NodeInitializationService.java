package com.dss.backend.engine.service;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.ConsensusAlgorithmFactory;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.VirtualNode;
import com.dss.backend.algorithm.failure.Heartbeat;
import com.dss.backend.model.Node;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.model.TopologyType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class NodeInitializationService {

    private final MessageRouter messageRouter;
    private final ScheduledExecutorService scheduler;

    // A dedicated worker pool for processing messages in VirtualNodes.
    private final ExecutorService workerPool;

    public NodeInitializationService(MessageRouter messageRouter, ScheduledExecutorService scheduler) {
        this.messageRouter = messageRouter;
        this.scheduler = scheduler;
        // can adjust the thread count as needed.
        this.workerPool = Executors.newFixedThreadPool(10);
    }

    /**
     * Initializes VirtualNode for each node by creating its consensus instance,
     * setting up heartbeat and phi-checker, and registering it with the MessageRouter.
     *
     * @param nodes       the list of nodes
     * @param config      the simulation configuration
     * @param topologyType the type of network topology (used later for neighbor mapping)
     * @return a Map of node IDs to their VirtualNode
     */
    public Map<String, VirtualNode> initializeNodes(List<Node> nodes, SimulationConfig config, TopologyType topologyType) {
        Map<String, VirtualNode> nodeMap = new ConcurrentHashMap<>();
        List<String> allNodeIds = nodes.stream()
                .map(Node::getId)
                .collect(Collectors.toList());

        for (Node node : nodes) {
            String nodeId = node.getId();
            ConsensusAlgorithm consensus = ConsensusAlgorithmFactory.createAlgorithm(
                    nodeId,
                    allNodeIds,
                    config,
                    messageRouter,
                    scheduler // Inject the scheduler for scheduling tasks (timeouts, phi-checks, etc.)
            );
            // Create a VirtualNode using the workerPool and scheduler.
            VirtualNode vNode = new VirtualNode(node, consensus, messageRouter, workerPool, scheduler);

            // Setup heartbeat using the shared scheduler.
            Heartbeat heartbeat = new Heartbeat(messageRouter, nodeId);
            vNode.setHeartbeat(heartbeat);
            heartbeat.start(scheduler);

            // Start the VirtualNode processing (this schedules the message loop and phi-checker).
            vNode.start();

            // Register with the MessageRouter.
            messageRouter.registerNode(nodeId, vNode);
            nodeMap.put(nodeId, vNode);
        }
        return nodeMap;
    }
}