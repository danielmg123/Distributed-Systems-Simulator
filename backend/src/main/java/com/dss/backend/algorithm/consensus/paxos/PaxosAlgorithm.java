package com.dss.backend.algorithm.consensus.paxos;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.engine.concurrent.MessageType;

/**
 * Paxos implementation that merges information from all PROMISEs:
 *   - We store data for each proposalNumber separately in ProposerData.
 *   - If any acceptor has an acceptedValue with a higher acceptedId, 
 *     we adopt that value for Phase 2.
 */
public class PaxosAlgorithm implements ConsensusAlgorithm {

    // Tracks per-node Paxos status (promisedId, acceptedId, etc.)
    private final PaxosState paxosState;

    // For sending messages between nodes
    private final MessageRouter router;

    // All participants in the cluster
    private final List<String> allNodeIds;

    // This node's unique ID
    private final String myNodeId;

    // Local counter for generating unique proposal numbers
    private final AtomicInteger proposalCounter = new AtomicInteger(0);

    // The number of nodes needed for majority
    private final int majority;

    // For each proposalNumber we (as Proposer) start, we store ProposerData
    private final ConcurrentHashMap<Integer, ProposerData> proposalDataMap = new ConcurrentHashMap<>();

    // Accept-count for each proposalNumber (Phase 2)
    private final ConcurrentHashMap<Integer, Integer> acceptCountMap = new ConcurrentHashMap<>();

    public PaxosAlgorithm(String myNodeId, List<String> allNodeIds, MessageRouter router) {
        this.myNodeId = myNodeId;
        this.allNodeIds = allNodeIds;
        this.router = router;
        this.paxosState = new PaxosState(myNodeId);

        this.majority = (allNodeIds.size() / 2) + 1;
    }

    @Override
    public void propose(Object value) {
        int proposalNumber = generateNextProposalNumber();

        // Initialize the data structure for tracking Phase 1 (PROMISE) replies
        ProposerData data = new ProposerData(value);
        proposalDataMap.put(proposalNumber, data);

        broadcastPrepareRequest(proposalNumber, value);
    }

    @Override
    public boolean accept(Object proposal) {
        // Not directly used. We handle ACCEPT_REQUEST via handleMessage().
        return false;
    }

    @Override
    public void commit(Object value) {
        // Mark the chosen value in local state
        paxosState.setChosenValue(value);
        System.out.println("Node " + myNodeId + " has COMMITTED value: " + value);

        // Optionally broadcast a final "CHOSEN" or "COMMIT" message 
        // so other nodes can learn the result.
        // broadcastCommit(proposalNumber, value);
    }

    // Called by the VirtualNodeThread when a message arrives
    public void handleMessage(SimulationMessage msg) {
        if (!(msg.getPayload() instanceof PaxosPayload)) {
            return; // not a Paxos message
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
                // (OPTIONAL) handle COMMIT or other messages
                break;
        }
    }

    // ---------------------- Phase 1: Prepare / Promise ----------------------
    private void broadcastPrepareRequest(int proposalNumber, Object originalValue) {
        // We attach the originalValue in case no acceptor has a prior acceptedValue
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(originalValue);

        for (String nodeId : allNodeIds) {
            SimulationMessage msg = new SimulationMessage(
                myNodeId,
                nodeId,
                MessageType.PREPARE_REQUEST,
                payload
            );
            router.messageSent(msg);
        }
    }

    private void onPrepareRequest(String sourceNode, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();

        // If proposalNumber >= promisedId, we promise not to accept anything below it
        if (proposalNumber >= paxosState.getPromisedId()) {
            paxosState.setPromisedId(proposalNumber);

            // Send PROMISE back to proposer
            PaxosPayload reply = new PaxosPayload();
            reply.setProposalNumber(proposalNumber);
            // Return our currently accepted proposal info, if any
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
        // else: we could send a REJECT message if we want, or just ignore
    }

    private void onPromise(String sourceNode, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        ProposerData data = proposalDataMap.get(proposalNumber);
        if (data == null) {
            // Possibly a stale or duplicated message. Ignore.
            return;
        }

        // 1. Bump the count of promises
        data.promiseCount += 1;

        // 2. Check if the acceptor has a previously accepted proposal
        int acceptedId = payload.getAcceptedId();
        Object acceptedValue = payload.getAcceptedValue();

        // If the acceptor has accepted something with the highest acceptedId so far, adopt it.
        if (acceptedId > data.highestAcceptedId) {
            data.highestAcceptedId = acceptedId;
            data.highestAcceptedValue = acceptedValue;
        }

        // 3. If we reached a majority, move to Phase 2
        if (data.promiseCount >= majority) {
            // Decide what value to use for Accept:
            // If no acceptor had an accepted proposal, we use our originalProposedValue.
            Object finalValue;
            if (data.highestAcceptedId > -1 && data.highestAcceptedValue != null) {
                finalValue = data.highestAcceptedValue;
            } else {
                finalValue = data.originalValue;
            }

            broadcastAcceptRequest(proposalNumber, finalValue);
        }
    }

    // ---------------------- Phase 2: Accept / Accepted ----------------------
    private void broadcastAcceptRequest(int proposalNumber, Object value) {
        // Re-initialize our acceptCount (so if we see multiple proposals in parallel, each has its own count)
        acceptCountMap.put(proposalNumber, 0);

        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);

        for (String nodeId : allNodeIds) {
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

        // If proposalNumber >= promisedId, accept
        if (proposalNumber >= paxosState.getPromisedId()) {
            paxosState.setAcceptedId(proposalNumber);
            paxosState.setAcceptedValue(payload.getProposedValue());

            // Send ACCEPTED
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
        // else: we could send a REJECT or ignore
    }

    private void onAccepted(String sourceNode, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        Integer oldVal = acceptCountMap.get(proposalNumber);
        if (oldVal == null) {
            // Possibly stale
            return;
        }

        int newVal = oldVal + 1;
        acceptCountMap.put(proposalNumber, newVal);

        // Check majority
        if (newVal >= majority) {
            // The value is chosen
            commit(payload.getProposedValue());

            // (Optional) Mark that this proposalNumber is complete to ignore further messages.
            // e.g... remove from data structures
        }
    }

    // ---------- Helper: Generate Unique Proposal IDs ----------
    private int generateNextProposalNumber() {
        // If each node has a numeric ID, we can combine them for uniqueness across cluster
        // For now, just do localCount
        return proposalCounter.incrementAndGet();
    }

    // Optional final "commit" broadcast
    /*
    private void broadcastCommit(int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);

        for (String nodeId : allNodeIds) {
            SimulationMessage msg = new SimulationMessage(
                myNodeId,
                nodeId,
                MessageType.COMMIT,
                payload
            );
            router.messageSent(msg);
        }
    }

    private void onCommitMessage(String sourceNode, PaxosPayload payload) {
        paxosState.setChosenValue(payload.getProposedValue());
        System.out.println("Node " + myNodeId + " learned CHOSEN value: " + payload.getProposedValue());
    }
    */

    /**
     * Data structure for Phase 1 at the proposer:
     * We store how many PROMISEs we have, the original proposed value,
     * and track the highest accepted proposal among all PROMISE responses.
     */
    private static class ProposerData {
        // The value this proposer started with in propose()
        final Object originalValue;

        // How many PROMISEs received so far
        int promiseCount = 0;

        // The highest acceptedId seen among all PROMISEs
        int highestAcceptedId = -1;

        // The value associated with that highest acceptedId
        Object highestAcceptedValue = null;

        ProposerData(Object originalValue) {
            this.originalValue = originalValue;
        }
    }
}
