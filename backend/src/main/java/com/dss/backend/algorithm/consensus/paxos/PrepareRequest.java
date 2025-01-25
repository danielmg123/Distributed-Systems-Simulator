package com.dss.backend.algorithm.consensus.paxos;

public class PrepareRequest {
    private final int proposalId;

    public PrepareRequest(int proposalId) {
        this.proposalId = proposalId;
    }

    public int getProposalId() {
        return proposalId;
    }
}
