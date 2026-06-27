package com.dss.backend.consensus;

import com.dss.backend.messaging.SimulationMessage;

/**
 * Defines the contract for all consensus algorithms (e.g. Paxos, Raft).
 * <p>
 * Implementations should provide logic for:
 * <ul>
 *   <li>{@link #propose(Object)} - how a node proposes a new value or operation.</li>
 *   <li>{@link #accept(Object)} - the acceptance logic for a new proposal.</li>
 *   <li>{@link #commit(Object)} - final commitment when a value/operation is chosen.</li>
 *   <li>{@link #handleMessage(SimulationMessage)} - message handling for protocol-specific phases.</li>
 * </ul>
 */
public interface ConsensusAlgorithm {

    /**
     * Initiates the proposal of a new value or operation in the consensus protocol.
     * <p>
     * For a leader-based protocol (like Raft or Multi-Paxos), this is typically
     * invoked by the leader node. Leaderless protocols (like basic Paxos) might allow
     * any node to propose.
     *
     * @param value the value or operation being proposed
     */
    void propose(Object value);

    /**
     * Indicates that this node has received a proposal and is deciding whether to accept it.
     * <p>
     * For example, in Paxos, nodes check whether the proposal number is >= promised number.
     *
     * @param proposal an object encapsulating details like proposal number, proposed value, etc.
     * @return true if this node accepts the proposal, false otherwise
     */
    boolean accept(Object proposal);

    /**
     * Performs final commitment once a proposal has been successfully chosen or replicated.
     * <p>
     * Typically, a committed value is considered safe to apply to the state machine
     * or the application.
     *
     * @param value the value being committed
     */
    void commit(Object value);

    /**
     * Processes an incoming {@link SimulationMessage} relevant to the consensus protocol.
     * <p>
     * This method should handle all protocol-specific message types:
     * <ul>
     *   <li>Paxos: PREPARE_REQUEST, PROMISE, ACCEPT_REQUEST, ACCEPTED, etc.</li>
     *   <li>Raft: REQUEST_VOTE, APPEND_ENTRIES, etc.</li>
     * </ul>
     *
     * @param msg the incoming message containing protocol data
     */
    void handleMessage(SimulationMessage msg);

    /**
     * Starts any background activity the algorithm needs while the node is active --
     * e.g. Raft's randomized election timer. Called by {@code VirtualNode.start()}.
     * Default no-op, since most protocols here are purely message-driven and need no
     * background timers of their own.
     */
    default void start() {
    }

    /**
     * Stops any background activity started by {@link #start()}. Called by
     * {@code VirtualNode.stop()} (including on {@code failNode()}), so a failed node's
     * algorithm-level timers go silent along with its message processing and heartbeat
     * -- a failed node must be deaf and mute, not just unreachable. Default no-op.
     */
    default void stop() {
    }

    /**
     * Returns a short, protocol-specific label describing this node's current role
     * within the protocol (e.g. Raft's "LEADER"/"CANDIDATE"/"FOLLOWER"), for display in
     * a dashboard. Default {@code null}, since most protocols here don't have a
     * meaningful per-node role concept the way leader-election protocols do.
     *
     * @return a role label, or {@code null} if this protocol has no such concept
     */
    default String getRoleLabel() {
        return null;
    }
}