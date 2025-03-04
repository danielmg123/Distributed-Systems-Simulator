package com.dss.backend.consensus.view_stamped_replication;

import com.dss.backend.consensus.ConsensusAlgorithm;
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

public class ViewStampedReplication implements ConsensusAlgorithm {

    private final AppLogger appLogger = new DefaultAppLogger(ViewStampedReplication.class);

    // Current view (for simplicity, starts at 0)
    private int view = 0;
    // Operation counter: each new proposal gets a unique op number.
    private int opNum = 0;
    // Last committed operation number
    private int committedOpNum = 0;

    // Pending proposals (op number to proposed value)
    private final Map<Integer, Object> pendingOps = new ConcurrentHashMap<>();
    // Count of acknowledgments received per op number.
    private final Map<Integer, Integer> ackCount = new ConcurrentHashMap<>();

    // --- Dependency Setters ---
    // External dependencies and configuration:
    @Setter
    private MessageRouter messageRouter;
    @Setter
    private String nodeId;
    @Setter
    private int totalNodes;
    // Flag indicating whether this node is the primary (leader) for the current view.
    private boolean isPrimary = false;

    private ConsensusBroadcaster broadcaster;

    // In a setter or initialization method, initialize the broadcaster.
    public void initBroadcaster() {
        this.broadcaster = new ConsensusBroadcaster(messageRouter, nodeId);
    }

    public void setPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
    }

    // --- ConsensusAlgorithm Methods ---

    /**
     * The primary node initiates a proposal.
     * It increments the op number, stores the value, and broadcasts a PREPARE message.
     */
    @Override
    public void propose(Object value) {
        if (!isPrimary) {
            appLogger.info("Non-primary node {} cannot initiate proposal; forward to primary.", nodeId);
            return;
        }
        opNum++;
        pendingOps.put(opNum, value);
        ackCount.put(opNum, 1); // Count self ack.
        VsrPayload payload = new VsrPayload(MessageType.PREPARE, view, opNum, value);
        // Use the broadcaster to send the PREPARE message.
        broadcaster.broadcast(MessageType.PREPARE, payload);
        appLogger.info("Primary {} initiated PREPARE for op #{} with value: {}", nodeId, opNum, value);
    }

    /**
     * The accept() method is not used in the VSR normal-case; backups respond directly in handleMessage.
     */
    @Override
    public boolean accept(Object proposal) {
        return false;
    }

    /**
     * Commits the operation and updates the committed operation number.
     */
    @Override
    public void commit(Object value) {
        committedOpNum++;
        appLogger.info("Node {} commits op #{} with value: {}", nodeId, committedOpNum, value);
    }

    /**
     * Handles incoming messages. Depending on the VSR message type, the node will:
     * - For PREPARE messages (backups): reply with a PREPARE_RESPONSE.
     * - For PREPARE_RESPONSE messages (primary): count responses and commit when quorum is reached.
     * - For COMMIT messages: update its committed operation.
     */
    @Override
    public void handleMessage(SimulationMessage msg) {
        if (!(msg.getPayload() instanceof VsrPayload)) {
            appLogger.info("Node {} received an unsupported payload: {}", nodeId, msg.getPayload());
            return;
        }
        VsrPayload payload = (VsrPayload) msg.getPayload();
        switch (payload.getType()) {
            case PREPARE:
                handlePrepare(msg.getSourceNodeId(), payload);
                break;
            case PREPARE_RESPONSE:
                handlePrepareResponse(msg.getSourceNodeId(), payload);
                break;
            case COMMIT:
                handleCommit(msg.getSourceNodeId(), payload);
                break;
            default:
                appLogger.info("Node {} received unknown VSR message type: {}", nodeId, payload.getType());
        }
    }

    // --- VSR Message Handlers ---

    /**
     * Handles incoming PREPARE messages.
     * Backup nodes respond immediately with a PREPARE_RESPONSE.
     */
    private void handlePrepare(String sourceNodeId, VsrPayload payload) {
        if (payload.getView() != view) {
            appLogger.info("Node {} ignoring PREPARE with mismatched view {}", nodeId, payload.getView());
            return;
        }
        int receivedOp = payload.getOpNum();
        Object proposedValue = payload.getProposedValue();
        appLogger.info("Node {} received PREPARE for op #{} with value: {} from {}", nodeId, receivedOp, proposedValue, sourceNodeId);
        // Reply with a PREPARE_RESPONSE.
        VsrPayload response = new VsrPayload(MessageType.PREPARE_RESPONSE, view, receivedOp, proposedValue);
        SimulationMessage responseMsg = SimulationMessageFactory.createMessage(nodeId, sourceNodeId, MessageType.PREPARE_RESPONSE, response);
        messageRouter.messageSent(responseMsg);
        appLogger.info("Node {} sent PREPARE_RESPONSE for op #{} to {}", nodeId, receivedOp, sourceNodeId);
    }

    /**
     * Handles incoming PREPARE_RESPONSE messages.
     * The primary counts acks; once a quorum is reached, it broadcasts a COMMIT message.
     */
    private void handlePrepareResponse(String sourceNodeId, VsrPayload payload) {
        if (!isPrimary) {
            appLogger.info("Non-primary node {} received PREPARE_RESPONSE; ignoring.", nodeId);
            return;
        }
        if (payload.getView() != view) {
            appLogger.info("Primary {} ignoring PREPARE_RESPONSE with mismatched view {}", nodeId, payload.getView());
            return;
        }
        int responseOp = payload.getOpNum();
        if (!pendingOps.containsKey(responseOp)) {
            appLogger.info("Primary {} received PREPARE_RESPONSE for unknown op #{}", nodeId, responseOp);
            return;
        }

        int count = ackCount.getOrDefault(responseOp, 0) + 1;
        ackCount.put(responseOp, count);
        appLogger.info("Primary {} received PREPARE_RESPONSE for op #{} from {} (ack count = {})", nodeId, responseOp, sourceNodeId, count);

        // Define quorum as ⌊totalNodes/2⌋ + 1.
        int quorum = (totalNodes / 2) + 1;
        if (count >= quorum) {
            Object valueToCommit = pendingOps.get(responseOp);
            // Broadcast COMMIT message to all backups.
            VsrPayload commitPayload = new VsrPayload(MessageType.COMMIT, view, responseOp, valueToCommit);
            broadcaster.broadcast(MessageType.COMMIT, commitPayload);

            // Primary commits locally.
            commit(valueToCommit);

            // Clean up the pending operation.
            pendingOps.remove(responseOp);
            ackCount.remove(responseOp);
        }
    }

    /**
     * Handles COMMIT messages.
     * Upon receiving a COMMIT, a node applies the operation if it has not been committed already.
     */
    private void handleCommit(String sourceNodeId, VsrPayload payload) {
        if (payload.getView() != view) {
            appLogger.info("Node {} ignoring COMMIT with mismatched view {}", nodeId, payload.getView());
            return;
        }
        int commitOp = payload.getOpNum();
        if (commitOp <= committedOpNum) {
            appLogger.info("Node {} already committed op #{}, ignoring COMMIT for op #{}", nodeId, committedOpNum, commitOp);
            return;
        }
        Object value = payload.getProposedValue();
        commit(value);
    }
}