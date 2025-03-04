package com.dss.backend.engine.concurrent;

import java.util.Map;
import java.util.concurrent.*;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.failure.Heartbeat;
import com.dss.backend.algorithm.failure.PhiAccrual;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import lombok.Getter;
import lombok.Setter;

public class VirtualNodeThread extends Thread {

    private final AppLogger appLogger = new DefaultAppLogger(VirtualNodeThread.class);

    private final Node node;
    private final ConsensusAlgorithm algorithm;
    private final MessageRouter router;
    @Getter
    @Setter
    private Heartbeat heartbeat;

    private final BlockingQueue<SimulationMessage> inboundQueue;
    private final Map<String, PhiAccrual> phiDetectors = new ConcurrentHashMap<>();

    private ScheduledFuture<?> phiCheckerFuture; // task scheduled on the central scheduler
    private final double phiThreshold = 8.0;

    private volatile boolean stopRequested = false;
    
    public VirtualNodeThread(Node node, ConsensusAlgorithm algo, MessageRouter router) {
        this.node = node;
        this.algorithm = algo;
        this.router = router;
        this.inboundQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        while (!stopRequested) {
            try {
                SimulationMessage msg = inboundQueue.take();
                try {
                    // Process HEARTBEAT messages separately.
                    if (msg.getType() == MessageType.HEARTBEAT && msg.getPayload() instanceof Long) {
                        long heartbeatTime = (Long) msg.getPayload();
                        // Update the PhiAccrual detector for the sender.
                        PhiAccrual detector = getOrCreatePhiDetectorFor(msg.getSourceNodeId());
                        detector.recordHeartbeat(heartbeatTime);
                    } else {
                        // Process normal consensus or other messages.
                        algorithm.handleMessage(msg);
                    }
                } catch (Exception e) {
                    // Log any exception thrown during message processing.
                    appLogger.error("Error processing message from {}: {}", msg.getSourceNodeId(), e.getMessage(), e);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }


    private PhiAccrual getOrCreatePhiDetectorFor(String neighborId) {
        return phiDetectors.computeIfAbsent(neighborId, id -> new PhiAccrual());
    }

    public void startPhiChecker(ScheduledExecutorService scheduler) {
        phiCheckerFuture = scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (Map.Entry<String, PhiAccrual> entry : phiDetectors.entrySet()) {
                double phi = entry.getValue().computePhi(now);
                if (phi >= phiThreshold) {
                    appLogger.info("Node {} suspects neighbor {} has failed (phi={})", node.getId(), entry.getKey(), phi);
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    public void stopPhiChecker() {
        if (phiCheckerFuture != null) {
            phiCheckerFuture.cancel(true);
        }
    }

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

    public String getNodeId() {return node.getId();}

    public NodeStatus getNodeStatus() {return node.getStatus();}
}