package com.dss.backend.engine.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;

public class VirtualNodeThread extends Thread {

    private final Node node;
    private final ConsensusAlgorithm algorithm;
    private final MessageRouter router;

    // inbound message queue for concurrency
    private final BlockingQueue<SimulationMessage> inboundQueue;

    private volatile boolean stopRequested = false;
    
    public VirtualNodeThread(Node node, ConsensusAlgorithm algo, MessageRouter router) {
        this.node = node;
        this.algorithm = algo;
        this.router = router;
        this.inboundQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run(){
       while(!stopRequested){
            try{
                // block until a message is avaliable
                SimulationMessage msg = inboundQueue.take();
                if (node.getStatus() == NodeStatus.FAILED){
                    // Node is failed, drop or ignore message
                    continue;
                }
                
                // ---- HOOK FOR RAFT OR PAXOS ----
                if (algorithm instanceof com.dss.backend.algorithm.consensus.raft.Raft raftImpl) {
                    raftImpl.handleMessage(msg);
                }
                else if (algorithm instanceof com.dss.backend.algorithm.consensus.paxos.PaxosAlgorithm paxosImpl) {
                    paxosImpl.handleMessage(msg);
                }
                else {
                    // fallback or do nothing
                }
            }
            catch(InterruptedException ex){
                Thread.currentThread().interrupt();
                break;
            }
       }
    }

    // private void processMessage(SimulationMessage msg){
    //     // example: if its a PROPOSAL or ACCEPT message, call the algorithm 
    //     switch(msg.getType()){
    //         case PROPOSAL:
    //             algorithm.propose(msg.getPayload());
    //             break;
    //         case ACCEPT:
    //             algorithm.accept(msg.getPayload());
    //             break;
    //         case COMMIT:
    //             algorithm.commit(msg.getPayload());
    //             break;
    //         default:
    //             break;
    //     }

    //     // possibly respond or broadcast new messages using router
    //     // ...
    // }

    public void enqueueMessage(SimulationMessage msg){
        inboundQueue.offer(msg);
    }

    public void requestStop(){
        stopRequested = true;
        this.interrupt(); // unblock inboundQueue.take()
    }

    public void failNode() {
        node.setStatus(NodeStatus.FAILED);
    }

    public void recoverNode() {
        node.setStatus(NodeStatus.ACTIVE);
    }
}
