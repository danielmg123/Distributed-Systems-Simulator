package com.dss.backend.engine.concurrent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.model.Node;

public class SimulationEngine {

    // A map of nodeId -> VirtualNodeThread
    private Map<String, VirtualNodeThread> nodeThreads = new ConcurrentHashMap<>();

    // message bus or router, if I choose to use a single central dispatcher
    private MessageRouter messageRouter;

    private volatile boolean running = false;

    public SimulationEngine(){
        // possibly pass in or create a shred MessageRouter
        this.messageRouter = new MessageRouter();
    }

    public void initializeNodes(List<Node> nodes, ConsensusAlgorithm algorithm){
        for(Node node : nodes){
            // create a virtual node thread for each node
            VirtualNodeThread vThread = new VirtualNodeThread(node, algorithm, messageRouter);
            messageRouter.registerNode(node.getId(), vThread);
            nodeThreads.put(node.getId(), vThread);
        }
    }

    public void startSimulation(){
        running = true;

        // start each nodes thread
        for(VirtualNodeThread vThread : nodeThreads.values()){
            vThread.start();
        }
    }

    public void stopSimulation(){
        // signal threads to stop
        running = false;
        for(VirtualNodeThread vThread : nodeThreads.values()){
            vThread.requestStop();
        }

        // Wait for threads to finish before returning
        for (VirtualNodeThread vThread : nodeThreads.values()) {
            try {
                vThread.join(); // Ensures all threads complete before exiting
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Interrupted while stopping simulation.");
            }
        }
    }

    // Additional functions to to inject failures, handle partitions ect...
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
}
