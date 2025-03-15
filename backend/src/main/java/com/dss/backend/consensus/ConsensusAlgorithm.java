package com.dss.backend.consensus;

import com.dss.backend.messaging.SimulationMessage;

/**
 * Defines the contract for all consensus algorithms (e.g. Paxos, Raft, ZAB).
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
     *   <li>ZAB: PROPOSAL, ACK, COMMIT, etc.</li>
     * </ul>
     *
     * @param msg the incoming message containing protocol data
     */
    void handleMessage(SimulationMessage msg);
}