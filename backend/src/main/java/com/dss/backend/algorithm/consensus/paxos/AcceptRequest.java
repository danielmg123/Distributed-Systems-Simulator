package com.dss.backend.algorithm.consensus.paxos;

public class AcceptRequest {
    private final int proposalId;
    private final Object value;

    public AcceptRequest(int proposalId, Object value) {
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

