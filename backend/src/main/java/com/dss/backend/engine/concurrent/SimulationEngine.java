package com.dss.backend.engine.concurrent;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.metrics.DefaultMetricsCollector;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.metrics.PerformanceMetricsCollector;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

public class SimulationEngine {

    // A map of nodeId -> VirtualNodeThread
    private Map<String, VirtualNodeThread> nodeThreads = new ConcurrentHashMap<>();

    // Message router for inter-node communication
    private MessageRouter messageRouter;

    private volatile boolean running = false;

    // Metrics Collector
    private final PerformanceMetricsCollector metricsCollector = new DefaultMetricsCollector();

    // Scheduler to trigger failure events periodically
    private final ScheduledExecutorService failureScheduler = Executors.newSingleThreadScheduledExecutor();
    private final Random random = new Random();

    public SimulationEngine(){
        this.messageRouter = new MessageRouter();
    }

    public void initializeNodes(List<Node> nodes, ConsensusAlgorithm algorithm){
        for(Node node : nodes){
            VirtualNodeThread vThread = new VirtualNodeThread(node, algorithm, messageRouter);
            messageRouter.registerNode(node.getId(), vThread);
            nodeThreads.put(node.getId(), vThread);
        }
    }

    public void startSimulation(){
        running = true;

        // Optionally, compute and log neighbor mapping based on topology.
        // (For example, if the SimulationConfig and NetworkTopology are provided elsewhere.)

        // Start each node thread.
        for(VirtualNodeThread vThread : nodeThreads.values()){
            vThread.start();
        }
    }

    /**
     * Starts a periodic task that randomly fails nodes based on the given failurePercentage.
     *
     * @param failurePercentage The percentage of nodes to fail (e.g., 10 for 10%).
     * @param intervalMillis    How frequently (in milliseconds) to trigger the failure check.
     */
    public void startFailureSimulation(double failurePercentage, long intervalMillis) {
        failureScheduler.scheduleAtFixedRate(() -> {
            for (VirtualNodeThread vThread : nodeThreads.values()) {
                // Only fail nodes that are ACTIVE.
                if (vThread.getNodeStatus() == NodeStatus.ACTIVE && random.nextDouble() < (failurePercentage / 100.0)) {
                    System.out.println("Simulating failure for node: " + vThread.getNodeId());
                    vThread.failNode();
                }
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    public void stopFailureSimulation() {
        failureScheduler.shutdownNow();
    }

    public void stopSimulation(){
        running = false;
        for(VirtualNodeThread vThread : nodeThreads.values()){
            vThread.requestStop();
        }
        stopFailureSimulation();
        for (VirtualNodeThread vThread : nodeThreads.values()) {
            try {
                vThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Interrupted while stopping simulation.");
            }
        }
    }

    public void failNode(String nodeId){
        VirtualNodeThread vThread = nodeThreads.get(nodeId);
        if(vThread != null){
            vThread.failNode();
        }
    }

    public void recoverNode(String nodeId){
        VirtualNodeThread vThread = nodeThreads.get(nodeId);
        if(vThread != null){
            vThread.recoverNode();
        }
    }

    // Method to get the current metrics snapshot
    public MetricsSnapshot getMetricsSnapshot() {
        return metricsCollector.getSnapshot();
    }
}
