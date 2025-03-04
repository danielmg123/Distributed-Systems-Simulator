package com.dss.backend.engine.service;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.ConsensusAlgorithmFactory;
import com.dss.backend.algorithm.failure.Heartbeat;
import com.dss.backend.config.SimulationProperties;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.VirtualNode;
import com.dss.backend.model.Node;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.model.TopologyType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class NodeInitializationService {

    private final MessageRouter messageRouter;
    private final Scheduler scheduler;
    private final ConsensusAlgorithmFactory consensusFactory;
    private final SimulationProperties simulationProperties;

    // Create a worker pool whose size is configurable.
    private final ExecutorService workerPool;

    public NodeInitializationService(MessageRouter messageRouter,
                                     Scheduler scheduler,
                                     ConsensusAlgorithmFactory consensusFactory,
                                     SimulationProperties simulationProperties) {
        this.messageRouter = messageRouter;
        this.scheduler = scheduler;
        this.consensusFactory = consensusFactory;
        this.simulationProperties = simulationProperties;
        this.workerPool = Executors.newFixedThreadPool(simulationProperties.getWorkerThreadPoolSize());
    }

    /**
     * Initializes VirtualNodes for each node by creating its consensus instance,
     * setting up heartbeat (with externalized interval) and starting the node,
     * then registering it with the MessageRouter.
     */
    public Map<String, VirtualNode> initializeNodes(List<Node> nodes,
                                                    SimulationConfig config,
                                                    TopologyType topologyType) {
        Map<String, VirtualNode> nodeMap = new ConcurrentHashMap<>();
        List<String> allNodeIds = nodes.stream()
                .map(Node::getId)
                .collect(Collectors.toList());

        for (Node node : nodes) {
            String nodeId = node.getId();
            // Create the consensus algorithm instance using the provided factory.
            ConsensusAlgorithm consensus = consensusFactory.createAlgorithm(nodeId, allNodeIds, config);
            // Create a VirtualNode using the shared workerPool and scheduler.
            VirtualNode vNode = new VirtualNode(node, consensus, messageRouter, workerPool, scheduler);

            // Create and start the heartbeat using the externalized heartbeat interval.
            Heartbeat heartbeat = new Heartbeat(messageRouter, nodeId, simulationProperties.getHeartbeatIntervalMillis());
            vNode.setHeartbeat(heartbeat);
            heartbeat.start(scheduler);

            // Start the VirtualNode (which begins its message loop and phi-checker).
            vNode.start();

            // Register the VirtualNode with the MessageRouter.
            messageRouter.registerNode(nodeId, vNode);
            nodeMap.put(nodeId, vNode);
        }
        return nodeMap;
    }
}