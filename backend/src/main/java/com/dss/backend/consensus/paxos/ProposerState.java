package com.dss.backend.consensus.paxos;

import lombok.Getter;

/**
 * Holds the state for a single proposal.
 */
@Getter
public class ProposerState {
    private final Object originalValue;
    private int promiseCount;
    private int highestAcceptedId;
    private Object highestAcceptedValue;

    public ProposerState(Object originalValue) {
        this.originalValue = originalValue;
        this.promiseCount = 0;
        this.highestAcceptedId = -1;
        this.highestAcceptedValue = null;
    }

    public void incrementPromiseCount() {
        this.promiseCount++;
    }

    /**
     * If the given acceptedId is higher than the current highest,
     * update the highest accepted state.
     */
    public void updateHighestAccepted(int acceptedId, Object acceptedValue) {
        if (acceptedId > this.highestAcceptedId && acceptedValue != null) {
            this.highestAcceptedId = acceptedId;
            this.highestAcceptedValue = acceptedValue;
        }
    }
}