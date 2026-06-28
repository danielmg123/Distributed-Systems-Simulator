package com.dss.backend.consensus;

/**
 * A hook that lets a consensus algorithm signal notable moments outward without
 * coupling it to metrics or event-logging concerns. The simulation wires in an
 * implementation (per run) that records metrics and/or logs events; tests and
 * standalone use can ignore it via {@link #NO_OP}.
 */
public interface ConsensusObserver {

    /** Does nothing; the default when no observer is wired in. */
    ConsensusObserver NO_OP = (nodeId, value) -> { };

    /**
     * Invoked once, at the cluster-agreement point, when a value is committed/chosen --
     * i.e. on the leader (Raft, Multi-Paxos) or proposer (Paxos) as it reaches a quorum,
     * not on every follower that later applies the value.
     *
     * @param nodeId the node that observed the value being committed
     * @param value  the committed value
     */
    void onCommitted(String nodeId, Object value);

    /**
     * Invoked when a node becomes leader of a new term. Only leader-election protocols
     * (Raft) call this. Default no-op.
     *
     * @param nodeId the node that became leader
     * @param term   the term it was elected for
     */
    default void onLeaderElected(String nodeId, int term) {
    }
}
