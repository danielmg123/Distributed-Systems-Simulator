package com.dss.backend.algorithm.consensus.view_stamped_replication;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.SimulationMessage;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ViewStampedReplication implements ConsensusAlgorithm {

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
            System.out.println("Non-primary node " + nodeId + " cannot initiate proposal; forward request to the primary.");
            return;
        }
        opNum++;
        pendingOps.put(opNum, value);
        // Primary counts itself as an acknowledgment.
        ackCount.put(opNum, 1);

        VsrPayload payload = new VsrPayload(MessageType.PREPARE, view, opNum, value);
        // Broadcast PREPARE message to all other nodes.
        for (String targetId : messageRouter.getRegisteredNodeIds()) {
            if (!targetId.equals(nodeId)) {
                SimulationMessage msg = new SimulationMessage(nodeId, targetId, null, payload);
                messageRouter.messageSent(msg);
            }
        }
        System.out.println("Primary " + nodeId + " initiated PREPARE for op #" + opNum + " with value: " + value);
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
        System.out.println("Node " + nodeId + " commits op #" + committedOpNum + " with value: " + value);
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
            System.out.println("Node " + nodeId + " received an unsupported payload: " + msg.getPayload());
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
                System.out.println("Node " + nodeId + " received unknown VSR message type: " + payload.getType());
        }
    }

    // --- VSR Message Handlers ---

    /**
     * Handles incoming PREPARE messages.
     * Backup nodes respond immediately with a PREPARE_RESPONSE.
     */
    private void handlePrepare(String sourceNodeId, VsrPayload payload) {
        if (payload.getView() != view) {
            System.out.println("Node " + nodeId + " ignoring PREPARE with mismatched view " + payload.getView());
            return;
        }
        int receivedOp = payload.getOpNum();
        Object proposedValue = payload.getProposedValue();
        System.out.println("Node " + nodeId + " received PREPARE for op #" + receivedOp +
                " with value: " + proposedValue + " from " + sourceNodeId);
        // Reply with a PREPARE_RESPONSE.
        VsrPayload response = new VsrPayload(MessageType.PREPARE_RESPONSE, view, receivedOp, proposedValue);
        SimulationMessage responseMsg = new SimulationMessage(nodeId, sourceNodeId, null, response);
        messageRouter.messageSent(responseMsg);
        System.out.println("Node " + nodeId + " sent PREPARE_RESPONSE for op #" + receivedOp + " to " + sourceNodeId);
    }

    /**
     * Handles incoming PREPARE_RESPONSE messages.
     * The primary counts acks; once a quorum is reached, it broadcasts a COMMIT message.
     */
    private void handlePrepareResponse(String sourceNodeId, VsrPayload payload) {
        if (!isPrimary) {
            System.out.println("Non-primary node " + nodeId + " received PREPARE_RESPONSE; ignoring.");
            return;
        }
        if (payload.getView() != view) {
            System.out.println("Primary " + nodeId + " ignoring PREPARE_RESPONSE with mismatched view " + payload.getView());
            return;
        }
        int responseOp = payload.getOpNum();
        if (!pendingOps.containsKey(responseOp)) {
            System.out.println("Primary " + nodeId + " received PREPARE_RESPONSE for unknown op #" + responseOp);
            return;
        }
        int count = ackCount.get(responseOp) + 1;
        ackCount.put(responseOp, count);
        System.out.println("Primary " + nodeId + " received PREPARE_RESPONSE for op #" + responseOp +
                " from " + sourceNodeId + " (ack count = " + count + ")");

        // Define quorum as ⌊totalNodes/2⌋ + 1.
        int quorum = (totalNodes / 2) + 1;
        if (count >= quorum) {
            Object valueToCommit = pendingOps.get(responseOp);
            // Broadcast COMMIT message to all backups.
            VsrPayload commitPayload = new VsrPayload(MessageType.COMMIT, view, responseOp, valueToCommit);
            for (String targetId : messageRouter.getRegisteredNodeIds()) {
                if (!targetId.equals(nodeId)) {
                    SimulationMessage commitMsg = new SimulationMessage(nodeId, targetId, null, commitPayload);
                    messageRouter.messageSent(commitMsg);
                }
            }
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
            System.out.println("Node " + nodeId + " ignoring COMMIT with mismatched view " + payload.getView());
            return;
        }
        int commitOp = payload.getOpNum();
        if (commitOp <= committedOpNum) {
            System.out.println("Node " + nodeId + " already committed op #" + committedOpNum +
                    ", ignoring COMMIT for op #" + commitOp);
            return;
        }
        Object value = payload.getProposedValue();
        commit(value);
    }
}