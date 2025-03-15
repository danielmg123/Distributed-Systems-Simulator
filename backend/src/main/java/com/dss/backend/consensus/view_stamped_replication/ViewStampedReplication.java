package com.dss.backend.consensus.view_stamped_replication;

import com.dss.backend.consensus.ConsensusAlgorithm;
import com.dss.backend.consensus.util.ConsensusBroadcaster;
import com.dss.backend.consensus.util.ConsensusUtils;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import com.dss.backend.messaging.*;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements <strong>View Stamped Replication (VSR)</strong>, a replication protocol
 * similar in spirit to Paxos and Raft, but using "views" (a primary replica in each view)
 * and a simple three-phase approach:
 * <ul>
 *   <li><strong>PREPARE</strong> (initiated by the primary),
 *   <li><strong>PREPARE_RESPONSE</strong> (backups acknowledge the prepare), and
 *   <li><strong>COMMIT</strong> once a quorum has acknowledged.</li>
 * </ul>
 *
 * <p><strong>Approach:</strong></p>
 * <ul>
 *   <li>A unique <em>view number</em> identifies each "configuration" or epoch.
 *       In normal operation, there is one primary (leader) in a given view.</li>
 *   <li>When the primary receives a new client operation, it issues a {@code PREPARE}
 *       to backups (other replicas). The backups respond with {@code PREPARE_RESPONSE}.</li>
 *   <li>Upon collecting enough responses (a quorum), the primary broadcasts a {@code COMMIT},
 *       and each node then applies (commits) the operation locally.</li>
 * </ul>
 *
 * <p><strong>Key Design Decisions:</strong></p>
 * <ul>
 *   <li><strong>Primary-based replication:</strong> Only the node designated as primary
 *       will initiate proposals. Other replicas wait for PREPARE messages, respond,
 *       and then commit upon receiving a COMMIT.</li>
 *   <li><strong>Operation numbers (opNum):</strong> Each new proposal increments
 *       an operation counter that is unique within a view. This ensures the backups
 *       can distinguish older from newer proposals.</li>
 *   <li><strong>High-level only:</strong> This example demonstrates the normal-case flow.
 *       It does not handle complex reconfiguration or view changes if the primary fails.</li>
 * </ul>
 *
 * <p><strong>Known Limitations:</strong></p>
 * <ul>
 *   <li>No automated view change. If the primary fails, the backup nodes in a real system
 *       would trigger a view change mechanism, but that is not implemented here.</li>
 *   <li>This simplified version does not handle partial failures or network partitions
 *       with the intricacy that a production VSR solution would require.</li>
 *   <li>No reconfiguration or membership changes at runtime.</li>
 * </ul>
 *
 * @author Daniel Morales
 */
public class ViewStampedReplication implements ConsensusAlgorithm {

    private final AppLogger appLogger = new DefaultAppLogger(ViewStampedReplication.class);

    /**
     * The current view identifier (for simplicity, starts at 0). A real system
     * would have logic to move to higher views upon primary failure.
     */
    private int view = 0;

    /**
     * The operation counter. Each new proposal increments this counter to
     * uniquely identify the proposal within the current view.
     */
    private int opNum = 0;

    /**
     * The highest operation number that this node has committed so far.
     */
    private int committedOpNum = 0;

    /**
     * Holds pending proposals. The key is the operation number, and the value is
     * the proposed command or data.
     */
    private final Map<Integer, Object> pendingOps = new ConcurrentHashMap<>();

    /**
     * Tracks how many acknowledgments have been received for each operation.
     * Key = opNum, Value = count of ACKs.
     */
    private final Map<Integer, Integer> ackCount = new ConcurrentHashMap<>();

    // ------------------------------------------------------------------------
    // External dependencies (injected via setters or constructor):
    // ------------------------------------------------------------------------

    /**
     * The message router used to send messages to other nodes.
     */
    @Setter
    private MessageRouter messageRouter;

    /**
     * The unique identifier of this node, e.g., "node1".
     */
    @Setter
    private String nodeId;

    /**
     * The total number of replicas. Used to compute a quorum.
     */
    @Setter
    private int totalNodes;

    /**
     * A flag that indicates whether this node is currently the primary (leader)
     * in this view.
     */
    private boolean isPrimary = false;

    /**
     * A helper utility for broadcasting messages to all other nodes.
     */
    private ConsensusBroadcaster broadcaster;

    /**
     * Initializes the {@link #broadcaster} once the {@link #messageRouter} and
     * {@link #nodeId} have been set. This should be called once the node is fully
     * configured (e.g., after the node ID is assigned).
     */
    public void initBroadcaster() {
        this.broadcaster = new ConsensusBroadcaster(messageRouter, nodeId);
    }

    /**
     * Sets whether this node is the primary. In a production system,
     * this would be managed by a separate view-change protocol.
     *
     * @param isPrimary true if this node is the primary for the current view.
     */
    public void setPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
    }

    // ------------------------------------------------------------------------
    // ConsensusAlgorithm interface methods
    // ------------------------------------------------------------------------

    /**
     * Called by the primary node to propose a new value. In normal operation,
     * only the primary should call {@code propose()}. This increments the
     * operation number, stores the proposal in {@link #pendingOps}, and
     * broadcasts a {@code PREPARE} to all backups.
     *
     * @param value The command or data being proposed for replication.
     */
    @Override
    public void propose(Object value) {
        // Only the primary can initiate proposals
        if (!isPrimary) {
            appLogger.info("Non-primary node {} cannot propose; forwarding to primary.", nodeId);
            return;
        }
        // Increment opNum for a new proposal
        opNum++;
        // Store the proposal in a pending map
        pendingOps.put(opNum, value);
        // Self-ack: the primary automatically counts as having acknowledged
        ackCount.put(opNum, 1);

        // Create the PREPARE payload
        VsrPayload payload = new VsrPayload(MessageType.PREPARE, view, opNum, value);
        // Broadcast to all other nodes
        broadcaster.broadcast(MessageType.PREPARE, payload, ProtocolType.VIEW_STAMPED_REPLICATION);

        appLogger.info("Primary {} initiated PREPARE for op #{} with value: {}", nodeId, opNum, value);
    }

    /**
     * In VSR, the {@code accept} step (familiar from Paxos) is not explicitly
     * separated in normal operation. We do not use accept() directly;
     * backups respond via {@link #handleMessage(SimulationMessage)}.
     *
     * @param proposal Ignored in VSR normal mode.
     * @return Always false in this simplified version.
     */
    @Override
    public boolean accept(Object proposal) {
        // VSR does not define a separate accept step in normal-case operation.
        return false;
    }

    /**
     * Marks the operation as committed locally. Once the primary sees that
     * a quorum has acknowledged the proposal, it sends a COMMIT message and
     * calls this method to apply the operation.
     *
     * @param value The data or command to apply.
     */
    @Override
    public void commit(Object value) {
        committedOpNum++;
        appLogger.info("Node {} commits op #{} with value: {}", nodeId, committedOpNum, value);
        // Application-specific logic (e.g., state machine apply) could go here.
    }

    /**
     * Handles incoming messages for VSR. We look at the {@link VsrPayload#getType()}
     * to see if it's a PREPARE, PREPARE_RESPONSE, or COMMIT. The node's reaction
     * depends on whether it's primary or a backup.
     *
     * @param msg The incoming SimulationMessage.
     */
    @Override
    public void handleMessage(SimulationMessage msg) {
        VsrPayload payload = ConsensusUtils.safeCastPayload(msg, VsrPayload.class);
        if (payload == null) {
            appLogger.info("Node {} received an unsupported payload: {}", nodeId, msg.getPayload());
            return;
        }
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
                break;
        }
    }

    // ------------------------------------------------------------------------
    // Internal message-handling routines
    // ------------------------------------------------------------------------

    /**
     * Handles a {@code PREPARE} message from the primary (normal-case).
     * <ul>
     *   <li>Updates local data structures (if needed).</li>
     *   <li>Immediately replies with a {@code PREPARE_RESPONSE} to the primary.</li>
     * </ul>
     *
     * @param sourceNodeId The ID of the node that sent the PREPARE (the primary).
     * @param payload       The VSR payload with {@code opNum} and proposed value.
     */
    private void handlePrepare(String sourceNodeId, VsrPayload payload) {
        if (payload.getView() != view) {
            // If the view does not match our current view, ignore or handle differently.
            appLogger.info("Node {} ignoring PREPARE with mismatched view {}", nodeId, payload.getView());
            return;
        }
        int receivedOp = payload.getOpNum();
        Object proposedValue = payload.getProposedValue();

        appLogger.info("Node {} received PREPARE for op #{} with value: {} from {}",
                nodeId, receivedOp, proposedValue, sourceNodeId);

        // Construct a PREPARE_RESPONSE to acknowledge we are ready for this op.
        VsrPayload response = new VsrPayload(MessageType.PREPARE_RESPONSE, view, receivedOp, proposedValue);
        SimulationMessage responseMsg = SimulationMessageFactory.createMessage(
                nodeId,
                sourceNodeId,
                MessageType.PREPARE_RESPONSE,
                response,
                ProtocolType.VIEW_STAMPED_REPLICATION
        );

        // Send the response back to the primary
        messageRouter.messageSent(responseMsg);

        appLogger.info("Node {} sent PREPARE_RESPONSE for op #{} to {}", nodeId, receivedOp, sourceNodeId);
    }

    /**
     * Handles a {@code PREPARE_RESPONSE} message at the primary. Each backup
     * sends one response to acknowledge the proposal. The primary counts these
     * acknowledgments and once it reaches a quorum, broadcasts a COMMIT.
     *
     * @param sourceNodeId The backup node that responded.
     * @param payload      The VSR payload containing the opNum and proposed value.
     */
    private void handlePrepareResponse(String sourceNodeId, VsrPayload payload) {
        // Only the primary processes PREPARE_RESPONSE. Backups ignore it.
        if (!isPrimary) {
            appLogger.info("Non-primary node {} received PREPARE_RESPONSE; ignoring.", nodeId);
            return;
        }
        // Check view consistency
        if (payload.getView() != view) {
            appLogger.info("Primary {} ignoring PREPARE_RESPONSE with mismatched view {}", nodeId, payload.getView());
            return;
        }
        int responseOp = payload.getOpNum();

        // Make sure we actually proposed this operation
        if (!pendingOps.containsKey(responseOp)) {
            appLogger.info("Primary {} received PREPARE_RESPONSE for unknown op #{}", nodeId, responseOp);
            return;
        }

        // Increment the ack count for this operation
        int count = ackCount.getOrDefault(responseOp, 0) + 1;
        ackCount.put(responseOp, count);

        appLogger.info("Primary {} received PREPARE_RESPONSE for op #{} from {} (ack count = {})",
                nodeId, responseOp, sourceNodeId, count);

        // Quorum = floor(totalNodes/2) + 1
        int quorum = (totalNodes / 2) + 1;
        if (count >= quorum) {
            // We have enough acks to commit
            Object valueToCommit = pendingOps.get(responseOp);

            // Broadcast COMMIT to all replicas
            VsrPayload commitPayload = new VsrPayload(MessageType.COMMIT, view, responseOp, valueToCommit);
            broadcaster.broadcast(MessageType.COMMIT, commitPayload, ProtocolType.VIEW_STAMPED_REPLICATION);

            // Commit locally on the primary
            commit(valueToCommit);

            // Clean up
            pendingOps.remove(responseOp);
            ackCount.remove(responseOp);
        }
    }

    /**
     * Handles a {@code COMMIT} message. When a node receives COMMIT,
     * it applies (commits) the operation if not already done.
     *
     * @param sourceNodeId The primary that sent the COMMIT (or another node forwarding it).
     * @param payload      The VSR payload with the opNum and proposed value to commit.
     */
    private void handleCommit(String sourceNodeId, VsrPayload payload) {
        if (payload.getView() != view) {
            appLogger.info("Node {} ignoring COMMIT with mismatched view {}", nodeId, payload.getView());
            return;
        }
        int commitOp = payload.getOpNum();

        // Avoid double-committing if we already advanced to a higher opNum
        if (commitOp <= committedOpNum) {
            appLogger.info("Node {} already committed op #{}, ignoring COMMIT for op #{}",
                    nodeId, committedOpNum, commitOp);
            return;
        }

        // Commit locally
        Object value = payload.getProposedValue();
        commit(value);
    }
}