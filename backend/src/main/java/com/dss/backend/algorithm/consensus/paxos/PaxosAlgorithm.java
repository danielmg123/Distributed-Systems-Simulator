package com.dss.backend.algorithm.consensus.paxos;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.engine.concurrent.MessageType;

/**
 * Paxos implementation that stores a PaxosState for each node.
 * Node concurrency is handled by VirtualNodeThread; 
 */
public class PaxosAlgorithm implements ConsensusAlgorithm {

    private final PaxosState paxosState;      // Per-node Paxos data
    private final MessageRouter router;       // For sending messages
    private final List<String> allNodeIds;    // All participants in the cluster
    private final String myNodeId;            // This nodes unique ID

    // A local counter for generating unique proposal numbers
    // (You can combine it with nodeId if needed.)
    private final AtomicInteger proposalCounter = new AtomicInteger(0);

    // Keep track of how many promises we've received for a given proposalNumber
    private final ConcurrentHashMap<Integer, Integer> promiseCount = new ConcurrentHashMap<>();

    public PaxosAlgorithm(String myNodeId, List<String> allNodeIds, MessageRouter router) {
        this.myNodeId = myNodeId;
        this.allNodeIds = allNodeIds;
        this.router = router;

        // Initialize the PaxosState for this node
        this.paxosState = new PaxosState(myNodeId);
    }

    @Override
    public void propose(Object value) {
        // Start Paxos Phase 1: Prepare
        int proposalNum = generateNextProposalNumber();
        broadcastPrepareRequest(proposalNum, value);
    }

    @Override
    public boolean accept(Object proposal) {
        // Might not be directly used in Paxos, might prefer separate method calls 
        // for "onAcceptRequest" etc...
        return false;
    }

    @Override
    public void commit(Object value) {
        // In Paxos, a "commit" or "chosen" phase can be signaled once I get a majority of ACCEPTED
        // might broadcast a "Learned" message or locally mark the consensus outcome 
    }

    /**
     * This method is invoked from the VirtualNodeThread when a message arrives.
     */
    public void handleMessage(SimulationMessage msg) {
        if (!(msg.getPayload() instanceof PaxosPayload)) {
            return; // Not a Paxos message or some error handling
        }

        PaxosPayload payload = (PaxosPayload) msg.getPayload();
        switch (msg.getType()) {
            case PREPARE_REQUEST:
                onPrepareRequest(msg.getSourceNodeId(), payload);
                break;
            case PROMISE:
                onPromise(msg.getSourceNodeId(), payload);
                break;
            case ACCEPT_REQUEST:
                onAcceptRequest(msg.getSourceNodeId(), payload);
                break;
            case ACCEPTED:
                onAccepted(msg.getSourceNodeId(), payload);
                break;
            default:
                // ignore or handle other types
                break;
        }
    }

    // ---------- Phase 1: Prepare / Promise ----------
    private void onPrepareRequest(String sourceNode, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();

        // If proposalNumber >= promisedId, promise not to accept proposals < proposalNumber
        if (proposalNumber >= paxosState.getPromisedId()) {
            paxosState.setPromisedId(proposalNumber);

            // Send PROMISE back to proposer
            PaxosPayload reply = new PaxosPayload();
            reply.setProposalNumber(proposalNumber);
            reply.setAcceptedId(paxosState.getAcceptedId());
            reply.setAcceptedValue(paxosState.getAcceptedValue());

            SimulationMessage promiseMsg = new SimulationMessage(
                myNodeId,
                sourceNode,
                MessageType.PROMISE,
                reply
            );
            router.messageSent(promiseMsg);
        }
        // else, ignore or send a reject message (not shown here).
    }

    private void onPromise(String sourceNode, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        promiseCount.merge(proposalNumber, 1, Integer::sum); // count one more promise

        // If the acceptor has accepted a proposal in the past, we might need to adopt that value
        // This is the "highest-numbered proposal" logic, omitted here for brevity.
        // e.g. compare payload.getAcceptedId() with the local record of the highest accepted.

        // Check if we have a majority
        int count = promiseCount.get(proposalNumber);
        int majority = (allNodeIds.size() / 2) + 1;
        if (count >= majority) {
            // Move to Phase 2: Accept
            // Possibly adopt the acceptedValue from the highest acceptedId, if any
            Object chosenValue = payload.getProposedValue(); 
            // might store or update that chosenValue from data collected

            broadcastAcceptRequest(proposalNumber, chosenValue);
        }
    }

    // ---------- Phase 2: Accept / Accepted ----------
    private void broadcastAcceptRequest(int proposalNumber, Object value) {
        for (String nodeId : allNodeIds) {
            PaxosPayload payload = new PaxosPayload();
            payload.setProposalNumber(proposalNumber);
            payload.setProposedValue(value);

            SimulationMessage msg = new SimulationMessage(
                myNodeId,
                nodeId,
                MessageType.ACCEPT_REQUEST,
                payload
            );
            router.messageSent(msg);
        }
    }

    private void onAcceptRequest(String sourceNode, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber >= paxosState.getPromisedId()) {
            // Accept it
            paxosState.setAcceptedId(proposalNumber);
            paxosState.setAcceptedValue(payload.getProposedValue());

            // Acknowledge
            PaxosPayload acceptedPayload = new PaxosPayload();
            acceptedPayload.setProposalNumber(proposalNumber);
            acceptedPayload.setProposedValue(payload.getProposedValue());

            SimulationMessage acceptedMsg = new SimulationMessage(
                myNodeId,
                sourceNode,
                MessageType.ACCEPTED,
                acceptedPayload
            );
            router.messageSent(acceptedMsg);
        }
        // else, ignore or send a reject
    }

    private void onAccepted(String sourceNode, PaxosPayload payload) {
        // Proposer sees that an acceptor accepted this proposalNumber
        // If we see a majority of ACCEPTED, we've "chosen" the value => "commit" 
        // For brevity, omitted. we can keep a separate acceptCount map similar to promiseCount.

        // example:
        // acceptCount.merge(payload.getProposalNumber(), 1, Integer::sum);
        // if acceptCount >= majority => commit(payload.getProposedValue())
    }

    private void broadcastPrepareRequest(int proposalNumber, Object value) {
        for (String nodeId : allNodeIds) {
            PaxosPayload payload = new PaxosPayload();
            payload.setProposalNumber(proposalNumber);
            payload.setProposedValue(value);

            SimulationMessage msg = new SimulationMessage(
                myNodeId,
                nodeId,
                MessageType.PREPARE_REQUEST,
                payload
            );
            router.messageSent(msg);
        }
    }

    private int generateNextProposalNumber() {
        // A naive approach:
        // Combine local counter with nodeId to ensure uniqueness across nodes, 
        // e.g.:  (counter << 8) + myNodeIdAsInt
        int localCount = proposalCounter.incrementAndGet();
        // If nodeId is numeric, we can do something like:
        // return (localCount << 16) | Integer.parseInt(myNodeId);
        // Or just do localCount if we know each nodeId is unique in some global sense
        return localCount;
    }
}
