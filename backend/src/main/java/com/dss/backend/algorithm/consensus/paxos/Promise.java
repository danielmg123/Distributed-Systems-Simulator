package com.dss.backend.algorithm.consensus.paxos;

public class Promise {
    private final int proposalId;
    private final int previousAcceptedId;
    private final Object previousAcceptedValue;

    public Promise(int proposalId, int previousAcceptedId, Object previousAcceptedValue) {
        this.proposalId = proposalId;
        this.previousAcceptedId = previousAcceptedId;
        this.previousAcceptedValue = previousAcceptedValue;
    }

    public int getProposalId() {
        return proposalId;
    }

    public int getPreviousAcceptedId() {
        return previousAcceptedId;
    }

    public Object getPreviousAcceptedValue() {
        return previousAcceptedValue;
    }
}
