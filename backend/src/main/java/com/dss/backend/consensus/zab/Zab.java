package com.dss.backend.consensus.zab;

import com.dss.backend.consensus.AbstractConsensusAlgorithm;
import com.dss.backend.consensus.util.ConsensusBroadcaster;
import com.dss.backend.consensus.util.ConsensusUtils;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import com.dss.backend.messaging.*;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements <strong>ZooKeeper Atomic Broadcast (Zab)</strong>, a protocol that underlies
 * Apache ZooKeeper’s replication and ordering guarantees.
 *
 * <p><strong>Approach (Normal Case):</strong></p>
 * <ul>
 *   <li>A single leader is responsible for issuing <em>proposals</em> with a unique
 *       transaction ID (zxid).</li>
 *   <li>Each follower, upon receiving a proposal, sends an <em>ACK</em> back to the leader.</li>
 *   <li>Once the leader obtains a quorum of ACKs (e.g., majority of followers),
 *       it <em>commits</em> the proposal and broadcasts a <em>COMMIT</em> message
 *       to all nodes so that each replica applies the transaction.</li>
 * </ul>
 *
 * <p><strong>Key Design Decisions:</strong></p>
 * <ul>
 *   <li><strong>Single Leader Model:</strong> Only one node (the leader) initiates
 *       proposals. Follower nodes are passive in that they simply ACK or commit
 *       as instructed.</li>
 *   <li><strong>Strict Ordering via zxid:</strong> Each proposal is assigned an
 *       incrementing zxid. This ensures a global total order on updates,
 *       which is crucial for distributed coordination in ZooKeeper.</li>
 *   <li><strong>Quorum-based Commit:</strong> Once the leader receives ACKs from
 *       a majority, the proposal is considered delivered and can be safely committed.</li>
 * </ul>
 *
 * <p><strong>Known Limitations in This Implementation:</strong></p>
 * <ul>
 *   <li>No <em>leader election</em>: We assume the leader is chosen externally
 *       or manually set with {@link #setLeader(boolean)}. If the leader fails,
 *       this code does not automatically switch to a new leader.</li>
 *   <li><em>Partial Failures and Partitions</em> are not fully handled:
 *       a real Zab implementation must detect unresponsive nodes and possibly
 *       trigger a re-election or re-sync process.</li>
 *   <li><em>Single-phase commit in normal operation</em>: The code does
 *       not demonstrate the recovery phase or synchronization that
 *       ZooKeeper does upon leader re-establishment.</li>
 * </ul>
 *
 * @author Daniel Morales
 */
public class Zab extends AbstractConsensusAlgorithm {

    private final AppLogger appLogger = new DefaultAppLogger(Zab.class);

    /** The shared message router used for sending messages to other nodes. */
    private MessageRouter router;

    /**
     * The unique identifier of this node (e.g., "node1"). Used to route
     * messages and log identification.
     */
    @Setter
    private String nodeId;

    /**
     * Total number of nodes in the cluster. Used to calculate the quorum
     * for commit decisions.
     */
    @Setter
    private int totalNodes;

    /**
     * Flag indicating whether this node is the leader in the Zab protocol.
     */
    private boolean isLeader = false;

    /**
     * Tracks the highest zxid used so far. Each new proposal increments this
     * counter, ensuring strictly increasing unique IDs for each transaction.
     */
    private long currentZxid = 0;

    /**
     * Stores proposals that have been sent but not yet committed.
     * Key = zxid, Value = the proposed data/value.
     */
    private final Map<Long, Object> pendingProposals = new ConcurrentHashMap<>();

    /**
     * Tracks how many ACKs have been received for each proposal.
     * Key = zxid, Value = count of ACK messages.
     */
    private final Map<Long, Integer> ackCounts = new ConcurrentHashMap<>();

    /**
     * Holds the most recently committed value for demonstration.
     * In a more complete system, multiple commits could be tracked in a log.
     */
    private Object committedValue;

    /**
     * A broadcaster to simplify sending messages to all other nodes in the cluster.
     */
    private ConsensusBroadcaster broadcaster;

    /**
     * Sets the MessageRouter dependency and initializes the broadcaster
     * for easy group messaging.
     *
     * @param router The message router for sending/receiving messages.
     */
    public void setMessageRouter(MessageRouter router) {
        this.router = router;
        this.broadcaster = new ConsensusBroadcaster(router, nodeId);
    }

    /**
     * Sets whether this node is the leader in Zab.
     *
     * @param isLeader true if this node acts as the leader.
     */
    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    /**
     * Propose a new value (transaction). Only the leader should call this:
     * <ul>
     *   <li>Increments the zxid.</li>
     *   <li>Saves the proposal in {@link #pendingProposals}.</li>
     *   <li>Broadcasts a {@code PROPOSAL} message to all other nodes.</li>
     * </ul>
     *
     * @param value The data/value to be broadcast and eventually committed.
     */
    @Override
    public void propose(Object value) {
        if (!isLeader) {
            // Follower's attempt to propose -> typically forward to leader or ignore.
            appLogger.info("Node {} is not leader. Forward proposal to leader.", nodeId);
            return;
        }

        // Generate a new, unique zxid
        currentZxid++;
        long zxid = currentZxid;

        // Record this proposal
        pendingProposals.put(zxid, value);
        // Leader self-ACKs
        ackCounts.put(zxid, 1);

        // Broadcast the PROPOSAL
        ZabPayload payload = new ZabPayload(MessageType.PROPOSAL, zxid, value);
        broadcaster.broadcast(MessageType.PROPOSAL, payload, ProtocolType.ZAB);

        appLogger.info("Leader {} proposed value '{}' with zxid {}", nodeId, value, zxid);
    }

    /**
     * For Zab, we do not use the {@code accept} method from the parent interface;
     * acceptance logic is handled via the normal {@link #handleMessage(SimulationMessage)} flow.
     */
    @Override
    public boolean accept(Object proposal) {
        // Zab handles acceptance inside handleMessage (follower receives PROPOSAL).
        return false;
    }

    /**
     * Called by the leader (and nodes receiving COMMIT) to finalize the proposal.
     * In this simplified approach, we store the last committed value for demonstration.
     *
     * @param value The data or transaction being committed.
     */
    @Override
    public void commit(Object value) {
        committedValue = value;
        appLogger.info("Node {} committed value: {}", nodeId, value);
        // Additional logic could apply a state machine transformation here.
    }

    /**
     * Processes all messages relevant to the Zab protocol:
     * <ul>
     *   <li>{@code PROPOSAL}: Follower receives from leader and sends an {@code ACK}.</li>
     *   <li>{@code ACK}: Leader collects acknowledgments. If enough are received, commits.</li>
     *   <li>{@code COMMIT}: All nodes mark the proposal as committed and apply it.</li>
     * </ul>
     *
     * @param msg The message containing a ZabPayload.
     */
    @Override
    public void handleMessage(SimulationMessage msg) {
        ZabPayload payload = ConsensusUtils.safeCastPayload(msg, ZabPayload.class);
        if (payload == null) {
            appLogger.info("Node {} received unsupported payload: {}", nodeId, msg.getPayload());
            return;
        }

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
                break;
        }
    }

    /**
     * Handles a PROPOSAL message from the leader:
     * <ul>
     *   <li>Follower records the proposal (zxid, value) if needed.</li>
     *   <li>Follower sends an {@code ACK} back to the leader immediately.</li>
     * </ul>
     *
     * @param sourceNodeId The leader that sent the proposal.
     * @param payload      The Zab payload with the zxid and proposed value.
     */
    private void handlePropose(String sourceNodeId, ZabPayload payload) {
        // A leader receiving its own PROPOSAL is typically unexpected but not harmful.
        if (isLeader) {
            appLogger.info("Leader {} received PROPOSE unexpectedly from {}.", nodeId, sourceNodeId);
            return;
        }

        long zxid = payload.getZxid();
        Object value = payload.getProposedValue();
        appLogger.info("Node {} received PROPOSE with zxid {} and value '{}' from {}",
                nodeId, zxid, value, sourceNodeId);

        // Send an ACK back to the leader
        ZabPayload ackPayload = new ZabPayload(MessageType.ACK, zxid, value);
        SimulationMessage ackMsg = SimulationMessageFactory.createMessage(
                nodeId, sourceNodeId, MessageType.ACK, ackPayload, ProtocolType.ZAB
        );
        router.messageSent(ackMsg);

        appLogger.info("Node {} sent ACK for zxid {} to {}", nodeId, zxid, sourceNodeId);
    }

    /**
     * Handles an ACK message (leader side):
     * <ul>
     *   <li>The leader increments the ack count for the given zxid.</li>
     *   <li>If the ack count reaches a quorum (majority), it sends {@code COMMIT}
     *       to the rest of the cluster and calls {@link #commit(Object)} locally.</li>
     * </ul>
     *
     * @param sourceNodeId The follower sending the acknowledgment.
     * @param payload      The Zab payload with the zxid and proposed value.
     */
    private void handleAck(String sourceNodeId, ZabPayload payload) {
        if (!isLeader) {
            appLogger.info("Non-leader {} received ACK; ignoring.", nodeId);
            return;
        }

        long zxid = payload.getZxid();
        // If we don't recognize this zxid, ignore.
        if (!pendingProposals.containsKey(zxid)) {
            appLogger.info("Leader {} received ACK for unknown zxid {}", nodeId, zxid);
            return;
        }

        // Increment ack count
        int newCount = ackCounts.get(zxid) + 1;
        ackCounts.put(zxid, newCount);

        appLogger.info("Leader {} received ACK from {} for zxid {} (ack count = {})",
                nodeId, sourceNodeId, zxid, newCount);

        // A quorum is (totalNodes/2 + 1)
        if (newCount >= ((totalNodes / 2) + 1)) {
            // Commit the proposal
            Object value = pendingProposals.get(zxid);
            // Broadcast COMMIT to all
            ZabPayload commitPayload = new ZabPayload(MessageType.COMMIT, zxid, value);
            broadcaster.broadcast(MessageType.COMMIT, commitPayload, ProtocolType.ZAB);

            // Leader commits locally
            commit(value);

            // Remove the proposal from pending
            pendingProposals.remove(zxid);
            ackCounts.remove(zxid);
        }
    }

    /**
     * Handles a COMMIT message (normal-case on both leader and followers):
     * <ul>
     *   <li>Mark the proposal associated with the given zxid as committed.</li>
     *   <li>In a more robust system, we could keep a log of all commits,
     *       but here we simply call {@link #commit(Object)} to store the
     *       latest committed value.</li>
     * </ul>
     *
     * @param sourceNodeId The node that sent the COMMIT (the leader).
     * @param payload      Contains the zxid and proposedValue to commit.
     */
    private void handleCommit(String sourceNodeId, ZabPayload payload) {
        long zxid = payload.getZxid();
        Object value = payload.getProposedValue();
        appLogger.info("Node {} received COMMIT for zxid {} with value '{}' from {}",
                nodeId, zxid, value, sourceNodeId);

        // Locally commit
        commit(value);
    }
}