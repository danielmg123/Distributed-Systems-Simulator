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
    private final ConsensusAlgorithmFactory consensusFactory;

    // A dedicated worker pool for processing messages in VirtualNodes.
    private final ExecutorService workerPool;

    public NodeInitializationService(MessageRouter messageRouter, ScheduledExecutorService scheduler,
                                     ConsensusAlgorithmFactory consensusFactory) {
        this.messageRouter = messageRouter;
        this.scheduler = scheduler;
        this.consensusFactory = consensusFactory;
        // Adjust the thread count as needed.
        this.workerPool = Executors.newFixedThreadPool(10);
    }

    /**
     * Initializes VirtualNode for each node by creating its consensus instance,
     * setting up heartbeat and phi-checker, and registering it with the MessageRouter.
     *
     * @param nodes        the list of nodes
     * @param config       the simulation configuration
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
            // Use the consensusFactory to create the consensus algorithm instance.
            ConsensusAlgorithm consensus = consensusFactory.createAlgorithm(nodeId, allNodeIds, config);
            // Create a VirtualNode with the shared worker pool and scheduler.
            VirtualNode vNode = new VirtualNode(node, consensus, messageRouter, workerPool, scheduler);

            // Setup and start heartbeat.
            Heartbeat heartbeat = new Heartbeat(messageRouter, nodeId);
            vNode.setHeartbeat(heartbeat);
            heartbeat.start(scheduler);

            // Start the VirtualNode processing (schedules its message loop and phi-checker).
            vNode.start();

            // Register the node with the MessageRouter.
            messageRouter.registerNode(nodeId, vNode);
            nodeMap.put(nodeId, vNode);
        }
        return nodeMap;
    }
}