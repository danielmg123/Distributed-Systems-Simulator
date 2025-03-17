package com.dss.backend.service.engine;

import com.dss.backend.consensus.ConsensusAlgorithm;
import com.dss.backend.consensus.ConsensusAlgorithmFactory;
import com.dss.backend.failure.Heartbeat;
import com.dss.backend.config.SimulationProperties;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.VirtualNode;
import com.dss.backend.model.Node;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.model.TopologyType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Responsible for instantiating and configuring the individual {@link VirtualNode}
 * instances that will participate in a simulation. This includes:
 * <ul>
 *   <li>Creating the appropriate {@link ConsensusAlgorithm} via a shared
 *       {@link ConsensusAlgorithmFactory}.</li>
 *   <li>Spinning up each {@code VirtualNode}'s heartbeat mechanism using
 *       the scheduler.</li>
 *   <li>Registering every node with the {@link MessageRouter}, so that
 *       message routing can function.</li>
 *   <li>Managing a dedicated worker pool for node-level tasks.</li>
 * </ul>
 *
 * <p>This service is usually called by a higher-level coordinator, such as
 * {@link com.dss.backend.service.engine.SimulationOrchestrator} or
 * {@link com.dss.backend.service.SimulationService}, to create all the nodes
 * before the simulation starts.
 */
public class NodeInitializationService {

    private final MessageRouter messageRouter;
    private final Scheduler scheduler;
    private final ConsensusAlgorithmFactory consensusFactory;
    private final SimulationProperties simulationProperties;

    // Worker pool to handle each node's internal tasks (e.g., concurrency, message processing).
    private final ExecutorService workerPool;

    /**
     * Constructs a {@link NodeInitializationService} with all the resources
     * needed to initialize nodes.
     *
     * @param messageRouter       shared router for sending/receiving messages
     * @param scheduler           central scheduling mechanism for tasks/timeouts
     * @param consensusFactory    factory to create the chosen consensus algorithm
     * @param simulationProperties general config (e.g., heartbeat interval, thread pool sizes)
     */
    public NodeInitializationService(MessageRouter messageRouter,
                                     Scheduler scheduler,
                                     ConsensusAlgorithmFactory consensusFactory,
                                     SimulationProperties simulationProperties) {
        this.messageRouter = messageRouter;
        this.scheduler = scheduler;
        this.consensusFactory = consensusFactory;
        this.simulationProperties = simulationProperties;

        // We create a distinct worker pool for parallel node initialization or message processing.
        this.workerPool = Executors.newFixedThreadPool(simulationProperties.getWorkerThreadPoolSize());
    }

    /**
     * Creates and starts a {@link VirtualNode} for each {@link Node} in the
     * provided list. Each {@code VirtualNode} is:
     * <ol>
     *   <li>Assigned a {@link ConsensusAlgorithm} from the {@code consensusFactory}.</li>
     *   <li>Registered with the {@code messageRouter} so it can send/receive messages.</li>
     *   <li>Starts a {@link Heartbeat} task on the central {@code scheduler}
     *       to broadcast heartbeats periodically.</li>
     *   <li>Begins its own internal message-processing loop.</li>
     * </ol>
     *
     * @param nodes        the list of domain {@link Node} objects
     * @param config       the simulation's configuration (consensus type, etc.)
     * @param topologyType the high-level topology type (RING, MESH, etc.)
     * @return a map of {@code nodeId} -> {@link VirtualNode}, which can be used
     *         for further coordination (e.g., shutting down nodes).
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

            // Create the consensus instance for this node.
            ConsensusAlgorithm consensus = consensusFactory.createAlgorithm(nodeId, allNodeIds, config);

            // Wrap it in a VirtualNode that uses the shared worker pool.
            VirtualNode vNode = new VirtualNode(node, consensus, messageRouter, workerPool, scheduler);

            // Create and start the heartbeat mechanism (using the configured interval).
            Heartbeat heartbeat = new Heartbeat(messageRouter, nodeId, simulationProperties.getHeartbeatIntervalMillis());
            vNode.setHeartbeat(heartbeat);
            heartbeat.start(scheduler);

            // Start the main VirtualNode's logic (e.g., queue processing).
            vNode.start();

            // Register it in the router so messages can find their target node.
            messageRouter.registerNode(nodeId, vNode);
            nodeMap.put(nodeId, vNode);
        }

        // (Optional) The topologyType can inform higher-level code if special neighbor links are required.
        return nodeMap;
    }
}