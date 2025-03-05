package com.dss.backend.consensus;

import com.dss.backend.messaging.SimulationMessage;

/**
 * This abstract base class provides default implementations for methods
 * that are not relevant to every consensus algorithm.
 *
 * Algorithms that do not use a particular phase (such as accept/commit) can
 * inherit these defaults instead of having to implement dummy methods.
 */
public abstract class AbstractConsensusAlgorithm implements ConsensusAlgorithm {

    @Override
    public void propose(Object value) {
        // Leave abstract: each algorithm must implement its own proposal logic.
        throw new UnsupportedOperationException("propose() must be implemented");
    }

    @Override
    public boolean accept(Object proposal) {return true;}

    @Override
    public void commit(Object value) {}

    @Override
    public abstract void handleMessage(SimulationMessage msg);
}