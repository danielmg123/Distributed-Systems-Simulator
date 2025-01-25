package com.dss.backend.algorithm.consensus.paxos;

public class Acceptor {
    private int lastPromisedId = -1;
    private int lastAcceptedId = -1;
    private Object lastAcceptedValue = null;

    public Promise receivePrepare(PrepareRequest request) {
        if (request.getProposalId() > lastPromisedId) {
            lastPromisedId = request.getProposalId();
            return new Promise(lastPromisedId, lastAcceptedId, lastAcceptedValue);
        }
        return null; // Reject if proposalId is not greater than lastPromisedId
    }

    public Accepted receiveAcceptRequest(AcceptRequest request) {
        if (request.getProposalId() >= lastPromisedId) {
            lastAcceptedId = request.getProposalId();
            lastAcceptedValue = request.getValue();
            return new Accepted(lastAcceptedId, lastAcceptedValue);
        }
        return null; // Reject if proposalId is not greater or equal to lastPromisedId
    }
}

