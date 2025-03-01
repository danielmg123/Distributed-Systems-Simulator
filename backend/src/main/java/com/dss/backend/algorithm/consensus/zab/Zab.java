package com.dss.backend.algorithm.consensus.zab;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.SimulationMessage;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class Zab implements ConsensusAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(Zab.class);

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

    // --- Dependency Setters ---
    public void setMessageRouter(MessageRouter router) {
        this.router = router;
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
            logger.info("Node {} is not leader. Cannot propose. Forward request to the leader.", nodeId);
            return;
        }
        // Increment zxid and save the proposal.
        currentZxid++;
        long zxid = currentZxid;
        pendingProposals.put(zxid, value);
        ackCounts.put(zxid, 1); // Leader counts as its own ACK.

        ZabPayload payload = new ZabPayload(MessageType.PROPOSAL, zxid, value);
        // Broadcast PROPOSE to all other nodes.
        for (String target : router.getRegisteredNodeIds()) {
            if (!target.equals(nodeId)) {
                SimulationMessage msg = new SimulationMessage(nodeId, target, null, payload);
                router.messageSent(msg);
            }
        }
        logger.info("Leader {} proposed value '{}' with zxid {}", nodeId, value, zxid);
    }

    @Override
    public boolean accept(Object proposal) {
        // Zab does not use an independent accept() method.
        throw new UnsupportedOperationException("Zab does not use accept() directly.");
    }

    @Override
    public void commit(Object value) {
        committedValue = value;
        logger.info("Node {} committed value: {}", nodeId, value);
    }

    @Override
    public void handleMessage(SimulationMessage msg) {
        if (!(msg.getPayload() instanceof ZabPayload)) {
            logger.info("Node {} received unsupported payload: {}", nodeId, msg.getPayload());
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
                logger.info("Node {} received unknown Zab message type: {}", nodeId, payload.getType());
        }
    }

    // --- Zab Message Handlers ---

    /**
     * Follower handling: upon receiving a PROPOSE message from the leader,
     * immediately send an ACK back.
     */
    private void handlePropose(String sourceNodeId, ZabPayload payload) {
        if (isLeader) {
            logger.info("Leader {} received PROPOSE message unexpectedly.", nodeId);
            return;
        }
        long zxid = payload.getZxid();
        Object value = payload.getProposedValue();
        logger.info("Node {} received PROPOSE with zxid {} and value '{}' from {}", nodeId, zxid, value, sourceNodeId);
        ZabPayload ackPayload = new ZabPayload(MessageType.ACK, zxid, value);
        SimulationMessage ackMsg = new SimulationMessage(nodeId, sourceNodeId, null, ackPayload);
        router.messageSent(ackMsg);
        logger.info("Node {} sent ACK for zxid {} to {}", nodeId, zxid, sourceNodeId);
    }

    /**
     * Leader handling: upon receiving an ACK, count it; once a quorum is reached,
     * broadcast a COMMIT message and commit the proposal locally.
     */
    private void handleAck(String sourceNodeId, ZabPayload payload) {
        if (!isLeader) {
            logger.info("Non-leader {} received ACK; ignoring.", nodeId);
            return;
        }
        long zxid = payload.getZxid();
        if (!pendingProposals.containsKey(zxid)) {
            logger.info("Leader {} received ACK for unknown zxid {}", nodeId, zxid);
            return;
        }
        int count = ackCounts.get(zxid) + 1;
        ackCounts.put(zxid, count);
        logger.info("Leader {} received ACK from {} for zxid {} (ack count = {})", nodeId, sourceNodeId, zxid, count);
        if (count >= getQuorum()) {
            Object value = pendingProposals.get(zxid);
            ZabPayload commitPayload = new ZabPayload(MessageType.COMMIT, zxid, value);
            // Broadcast COMMIT to all nodes.
            for (String target : router.getRegisteredNodeIds()) {
                if (!target.equals(nodeId)) {
                    SimulationMessage commitMsg = new SimulationMessage(nodeId, target, null, commitPayload);
                    router.messageSent(commitMsg);
                }
            }
            // Leader commits locally.
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
        logger.info("Node " + nodeId + " received COMMIT for zxid " + zxid + " with value '" + value + "' from " + sourceNodeId);
        commit(value);
    }
}