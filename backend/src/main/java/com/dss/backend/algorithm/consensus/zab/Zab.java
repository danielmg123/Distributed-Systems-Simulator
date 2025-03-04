package com.dss.backend.algorithm.consensus.zab;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.util.ConsensusBroadcaster;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Zab implements ConsensusAlgorithm {

    private final AppLogger appLogger = new DefaultAppLogger(Zab.class);

    private MessageRouter router;
    @Setter
    private String nodeId;
    @Setter
    private int totalNodes;
    private boolean isLeader = false;
    // Current transaction id (zxid) counter
    private long currentZxid = 0;

    // Map from zxid to proposed value
    private final Map<Long, Object> pendingProposals = new ConcurrentHashMap<>();
    // Map from zxid to ack count
    private final Map<Long, Integer> ackCounts = new ConcurrentHashMap<>();

    private Object committedValue;
    private ConsensusBroadcaster broadcaster;

    // --- Dependency Setters ---
    public void setMessageRouter(MessageRouter router) {
        this.router = router;
        this.broadcaster = new ConsensusBroadcaster(router, nodeId);
    }

    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    // Helper to compute quorum: ⌊totalNodes/2⌋ + 1
    private int getQuorum() {
        return (totalNodes / 2) + 1;
    }

    @Override
    public void propose(Object value) {
        if (!isLeader) {
            appLogger.info("Node {} is not leader. Forward proposal to leader.", nodeId);
            return;
        }
        currentZxid++;
        long zxid = currentZxid;
        pendingProposals.put(zxid, value);
        ackCounts.put(zxid, 1); // Count self ACK.
        ZabPayload payload = new ZabPayload(MessageType.PROPOSAL, zxid, value);
        // Use the broadcaster to send the proposal.
        broadcaster.broadcast(MessageType.PROPOSAL, payload);
        appLogger.info("Leader {} proposed value '{}' with zxid {}", nodeId, value, zxid);
    }

    @Override
    public boolean accept(Object proposal) {
        // Zab does not use an independent accept() method.
        throw new UnsupportedOperationException("Zab does not use accept() directly.");
    }

    @Override
    public void commit(Object value) {
        committedValue = value;
        appLogger.info("Node {} committed value: {}", nodeId, value);
    }

    @Override
    public void handleMessage(SimulationMessage msg) {
        if (!(msg.getPayload() instanceof ZabPayload)) {
            appLogger.info("Node {} received unsupported payload: {}", nodeId, msg.getPayload());
            return;
        }
        ZabPayload payload = (ZabPayload) msg.getPayload();
        switch (payload.getType()) {
            case PROPOSAL:
                handlePropose(msg.getSourceNodeId(), payload);
                break;
            case ACK:
                handleAck(msg.getSourceNodeId(), payload);
                break;
            case COMMIT:
                handleCommit(msg.getSourceNodeId(), payload);
                break;
            default:
                appLogger.info("Node {} received unknown Zab message type: {}", nodeId, payload.getType());
        }
    }

    // --- Zab Message Handlers ---

    /**
     * Follower handling: upon receiving a PROPOSE message from the leader,
     * immediately send an ACK back.
     */
    private void handlePropose(String sourceNodeId, ZabPayload payload) {
        if (isLeader) {
            appLogger.info("Leader {} received PROPOSE unexpectedly.", nodeId);
            return;
        }
        long zxid = payload.getZxid();
        Object value = payload.getProposedValue();
        appLogger.info("Node {} received PROPOSE with zxid {} and value '{}' from {}", nodeId, zxid, value, sourceNodeId);
        ZabPayload ackPayload = new ZabPayload(MessageType.ACK, zxid, value);
        SimulationMessage ackMsg = new SimulationMessage(nodeId, sourceNodeId, MessageType.ACK, ackPayload);
        router.messageSent(ackMsg);
        appLogger.info("Node {} sent ACK for zxid {} to {}", nodeId, zxid, sourceNodeId);
    }

    /**
     * Leader handling: upon receiving an ACK, count it; once a quorum is reached,
     * broadcast a COMMIT message and commit the proposal locally.
     */
    private void handleAck(String sourceNodeId, ZabPayload payload) {
        if (!isLeader) {
            appLogger.info("Non-leader {} received ACK; ignoring.", nodeId);
            return;
        }
        long zxid = payload.getZxid();
        if (!pendingProposals.containsKey(zxid)) {
            appLogger.info("Leader {} received ACK for unknown zxid {}", nodeId, zxid);
            return;
        }
        int count = ackCounts.get(zxid) + 1;
        ackCounts.put(zxid, count);
        appLogger.info("Leader {} received ACK from {} for zxid {} (ack count = {})", nodeId, sourceNodeId, zxid, count);
        if (count >= ((totalNodes / 2) + 1)) {
            Object value = pendingProposals.get(zxid);
            ZabPayload commitPayload = new ZabPayload(MessageType.COMMIT, zxid, value);
            broadcaster.broadcast(MessageType.COMMIT, commitPayload);
            commit(value);
            pendingProposals.remove(zxid);
            ackCounts.remove(zxid);
        }
    }

    /**
     * Upon receiving a COMMIT message, a node commits the proposal.
     */
    private void handleCommit(String sourceNodeId, ZabPayload payload) {
        long zxid = payload.getZxid();
        Object value = payload.getProposedValue();
        appLogger.info("Node {} received COMMIT for zxid {} with value '{}' from {}", nodeId, zxid, value, sourceNodeId);
        commit(value);
    }
}