package com.dss.backend.consensus.zab;

import com.dss.backend.consensus.AbstractConsensusAlgorithm;
import com.dss.backend.consensus.util.ConsensusBroadcaster;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.SimulationMessageFactory;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Zab (ZooKeeper Atomic Broadcast) consensus algorithm implementation.
 *
 * This class extends AbstractConsensusAlgorithm to inherit default (no‑op)
 * implementations for methods like accept(), which Zab does not use.
 * It implements its own logic for propose() and handleMessage() using consistent
 * message types.
 */
public class Zab extends AbstractConsensusAlgorithm {

    private final AppLogger appLogger = new DefaultAppLogger(Zab.class);

    private MessageRouter router;
    @Setter
    private String nodeId;
    @Setter
    private int totalNodes;
    private boolean isLeader = false;

    // Current transaction identifier (zxid) counter.
    private long currentZxid = 0;

    // Maps to track pending proposals and acknowledgment counts.
    private final Map<Long, Object> pendingProposals = new ConcurrentHashMap<>();
    private final Map<Long, Integer> ackCounts = new ConcurrentHashMap<>();

    private Object committedValue;
    private ConsensusBroadcaster broadcaster;

    /**
     * Sets the MessageRouter dependency and initializes the broadcaster.
     *
     * @param router the MessageRouter instance to use for routing messages.
     */
    public void setMessageRouter(MessageRouter router) {
        this.router = router;
        this.broadcaster = new ConsensusBroadcaster(router, nodeId);
    }

    /**
     * Sets whether this node is the leader.
     *
     * @param isLeader true if this node is the leader.
     */
    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    /**
     * Proposes a new value. Only the leader should initiate proposals.
     *
     * @param value the value to propose.
     */
    @Override
    public void propose(Object value) {
        if (!isLeader) {
            appLogger.info("Node {} is not leader. Forward proposal to leader.", nodeId);
            return;
        }
        currentZxid++;
        long zxid = currentZxid;
        pendingProposals.put(zxid, value);
        ackCounts.put(zxid, 1); // Count self acknowledgment.
        ZabPayload payload = new ZabPayload(MessageType.PROPOSAL, zxid, value);
        // Broadcast the proposal to all other nodes.
        broadcaster.broadcast(MessageType.PROPOSAL, payload);
        appLogger.info("Leader {} proposed value '{}' with zxid {}", nodeId, value, zxid);
    }

    // We do not override accept() so that the default no‑op implementation from
    // AbstractConsensusAlgorithm is used.

    /**
     * Commits a value. This method is called once a proposal has reached quorum.
     *
     * @param value the value to commit.
     */
    @Override
    public void commit(Object value) {
        committedValue = value;
        appLogger.info("Node {} committed value: {}", nodeId, value);
    }

    /**
     * Handles incoming simulation messages.
     *
     * @param msg the SimulationMessage to process.
     */
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
     * Handles a PROPOSAL message.
     * Follower nodes, upon receiving a proposal from the leader,
     * immediately send an ACK back.
     *
     * @param sourceNodeId the ID of the node that sent the proposal.
     * @param payload      the ZabPayload containing the proposal.
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
        SimulationMessage ackMsg = SimulationMessageFactory.createMessage(nodeId, sourceNodeId, MessageType.ACK, ackPayload);
        router.messageSent(ackMsg);
        appLogger.info("Node {} sent ACK for zxid {} to {}", nodeId, zxid, sourceNodeId);
    }

    /**
     * Handles an ACK message.
     * The leader counts ACKs for a proposal; once a quorum is reached,
     * it broadcasts a COMMIT message and commits the proposal locally.
     *
     * @param sourceNodeId the ID of the node sending the ACK.
     * @param payload      the ZabPayload containing the acknowledgment.
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
        // Define quorum as (totalNodes / 2) + 1.
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
     * Handles a COMMIT message.
     * When a node receives a COMMIT, it commits the proposed value.
     *
     * @param sourceNodeId the ID of the node that sent the COMMIT.
     * @param payload      the ZabPayload containing the commit information.
     */
    private void handleCommit(String sourceNodeId, ZabPayload payload) {
        long zxid = payload.getZxid();
        Object value = payload.getProposedValue();
        appLogger.info("Node {} received COMMIT for zxid {} with value '{}' from {}", nodeId, zxid, value, sourceNodeId);
        commit(value);
    }
}