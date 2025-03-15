package com.dss.backend.consensus.paxos;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * <strong>PaxosState</strong> holds the local per-node (Acceptor) state
 * for the Paxos protocol.
 * <p>
 * Each node must track:
 * <ul>
 *   <li>{@code promisedId}: the highest proposal number
 *       the node has promised not to accept proposals below.</li>
 *   <li>{@code acceptedId} and {@code acceptedValue}:
 *       the highest proposal and its associated value that the node has actually accepted.</li>
 *   <li>{@code chosenValue}: optionally, the final committed (chosen) value
 *       once the node learns the proposal is accepted by a quorum.</li>
 * </ul>
 * </p>
 * <p>
 * In basic Paxos, once a node has <em>chosenValue</em>, it can apply
 * this result to its local state machine or replica.
 * </p>
 *
 * @see com.dss.backend.consensus.paxos.PaxosAlgorithm
 */
@Data
@RequiredArgsConstructor
public class PaxosState {

    /**
     * The unique identifier of this node. In some Paxos variants, this
     * might be used to help generate globally unique proposal numbers (e.g. nodeId + localCounter).
     */
    private final String nodeId;

    /**
     * The highest proposal number we have promised not to accept proposals below.
     * If we get an ACCEPT_REQUEST with a lower number, we must reject it.
     */
    private int promisedId = -1;

    /** The highest proposal number we have accepted. */
    private int acceptedId = -1;

    /** The value associated with {@link #acceptedId}. */
    private Object acceptedValue = null;

    /**
     * The final chosen/committed value once we learn that a proposal
     * has been accepted by a majority.
     */
    private Object chosenValue = null;
}