package com.dss.backend.algorithm.consensus.paxos;

public class Accepted {
    private final int proposalId;
    private final Object value;

    public Accepted(int proposalId, Object value) {
        this.proposalId = proposalId;
        this.value = value;
    }

    public int getProposalId() {
        return proposalId;
    }

    public Object getValue() {
        return value;
    }
}