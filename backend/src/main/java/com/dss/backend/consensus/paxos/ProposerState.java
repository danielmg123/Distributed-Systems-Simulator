package com.dss.backend.consensus.paxos;

import lombok.Getter;

/**
 * <strong>ProposerState</strong> encapsulates the local state a Proposer keeps
 * while running Paxos <em>Phase 1</em> (Prepare/Promise).
 * <p>
 * It tracks how many PROMISE messages we've received thus far
 * ({@code promiseCount}), as well as the highest accepted proposal
 * from any Acceptor that responded ({@code highestAcceptedId}/{@code highestAcceptedValue}).
 * </p>
 * <p>
 * After collecting a quorum of PROMISE messages, the Proposer chooses
 * the <em>highest accepted value</em> among them if any node had a
 * previously accepted proposal. If none exists, it can use its original
 * proposal value.
 * </p>
 *
 * @see com.dss.backend.consensus.paxos.PaxosAlgorithm
 */
@Getter
public class ProposerState {

    /**
     * The original value the Proposer wanted to propose
     * (before learning about any previously accepted proposals).
     */
    private final Object originalValue;

    /** Number of PROMISE responses we have received so far. */
    private int promiseCount;

    /**
     * Tracks the highest acceptedId encountered in the PROMISE messages
     * from Acceptors (i.e., if any node had accepted a proposal with a
     * higher proposal number).
     */
    private int highestAcceptedId;

    /**
     * The value associated with the highestAcceptedId. If non-null,
     * we adopt this value as ours in Phase 2 to ensure consistency.
     */
    private Object highestAcceptedValue;

    /**
     * Creates a new ProposerState for a fresh proposal, starting with
     * no knowledge of previously accepted proposals.
     *
     * @param originalValue The value we want to propose initially.
     */
    public ProposerState(Object originalValue) {
        this.originalValue = originalValue;
        this.promiseCount = 0;
        this.highestAcceptedId = -1;
        this.highestAcceptedValue = null;
    }

    /** Increments the count of PROMISE messages received. */
    public void incrementPromiseCount() {
        this.promiseCount++;
    }

    /**
     * Updates the highest accepted info if the new {@code acceptedId}
     * is strictly greater than the current {@code highestAcceptedId}
     * (and the acceptedValue is not null).
     *
     * @param acceptedId    the proposal number from an Acceptor's existing acceptance
     * @param acceptedValue the value that Acceptor has accepted
     */
    public void updateHighestAccepted(int acceptedId, Object acceptedValue) {
        if (acceptedId > this.highestAcceptedId && acceptedValue != null) {
            this.highestAcceptedId = acceptedId;
            this.highestAcceptedValue = acceptedValue;
        }
    }
}