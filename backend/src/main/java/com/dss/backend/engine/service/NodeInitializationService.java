package com.dss.backend.engine.service;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.ConsensusAlgorithmFactory;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.VirtualNodeThread;
import com.dss.backend.algorithm.failure.Heartbeat;
import com.dss.backend.model.Node;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.model.TopologyType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class NodeInitializationService {

    private final MessageRouter messageRouter;
    private final ScheduledExecutorService scheduler;

    public NodeInitializationService(MessageRouter messageRouter, ScheduledExecutorService scheduler) {
        this.messageRouter = messageRouter;
        this.scheduler = scheduler;
    }

    /**
     * Initializes VirtualNodeThreads for each node by creating its consensus instance,
     * setting up heartbeat and phi-checker, and registering it with the MessageRouter.
     *
     * @param nodes       the list of nodes
     * @param config      the simulation configuration
     * @param topologyType the type of network topology (used later for neighbor mapping)
     * @return a Map of node IDs to their VirtualNodeThread
     */
    public Map<String, VirtualNodeThread> initializeNodes(List<Node> nodes, SimulationConfig config, TopologyType topologyType) {
        Map<String, VirtualNodeThread> nodeThreads = new ConcurrentHashMap<>();
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
                    scheduler // inject the scheduler for timeouts, etc.
            );
            VirtualNodeThread vThread = new VirtualNodeThread(node, consensus, messageRouter);

            // Setup heartbeat using the shared scheduler.
            Heartbeat heartbeat = new Heartbeat(messageRouter, nodeId);
            vThread.setHeartbeat(heartbeat);
            heartbeat.start(scheduler);

            // Start the phi-checker on the shared scheduler.
            vThread.startPhiChecker(scheduler);

            messageRouter.registerNode(nodeId, vThread);
            nodeThreads.put(nodeId, vThread);
        }
        return nodeThreads;
    }
}