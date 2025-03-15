package com.dss.backend.consensus;

import com.dss.backend.messaging.SimulationMessage;

/**
 * A base class providing default (often no-op) implementations of
 * {@link ConsensusAlgorithm}'s methods.
 * <p>
 * Subclasses can extend this to avoid implementing every method from scratch when
 * certain phases aren’t used by their protocol.
 */
public abstract class AbstractConsensusAlgorithm implements ConsensusAlgorithm {

    /**
     * Called when a node wants to propose a new value or command.
     * <p>
     * The default implementation throws {@link UnsupportedOperationException},
     * so subclasses should override this if they actively initiate proposals.
     *
     * @param value the value to propose
     */
    @Override
    public void propose(Object value) {
        throw new UnsupportedOperationException("propose() must be implemented");
    }

    /**
     * Called to check if this node will accept a proposal (e.g., Paxos accept or
     * Raft append). By default, this method returns <code>true</code> (meaning
     * “accept everything”).
     * <p>
     * Subclasses can override to implement logic such as checking proposal numbers
     * or log indices.
     *
     * @param proposal the proposal object
     * @return <code>true</code> by default
     */
    @Override
    public boolean accept(Object proposal) {
        return true;
    }

    /**
     * Called once a value or command is fully chosen/replicated and ready to be
     * applied or recorded as committed.
     * <p>
     * This default implementation is a no-op. Subclasses implementing a
     * replicated state machine should override to apply the commit to local state.
     *
     * @param value the committed value
     */
    @Override
    public void commit(Object value) {
        // Default no-op
    }

    /**
     * Processes a protocol-specific message. The default method throws an exception
     * because each algorithm must handle its own message types.
     * <p>
     * Examples:
     * <ul>
     *   <li>Paxos: PREPARE_REQUEST, PROMISE, ACCEPT_REQUEST, ACCEPTED, etc.</li>
     *   <li>Raft: REQUEST_VOTE, APPEND_ENTRIES, etc.</li>
     * </ul>
     *
     * @param msg the incoming message
     * @throws UnsupportedOperationException if not overridden by the subclass
     */
    @Override
    public void handleMessage(SimulationMessage msg) {
        throw new UnsupportedOperationException("handleMessage() must be implemented by subclasses");
    }
}