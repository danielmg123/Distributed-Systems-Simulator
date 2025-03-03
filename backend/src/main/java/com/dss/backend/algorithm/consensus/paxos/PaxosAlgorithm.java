package com.dss.backend.algorithm.consensus.paxos;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.util.ConsensusBroadcaster;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.SimulationMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Paxos implementation that merges information from all PROMISEs:
 *   - We store data for each proposalNumber separately in ProposerData.
 *   - If any acceptor has an acceptedValue with a higher acceptedId, 
 *     we adopt that value for Phase 2.
 */
public class PaxosAlgorithm implements ConsensusAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(PaxosAlgorithm.class);

    // Tracks per-node Paxos status (promisedId, acceptedId, etc.)
    private final PaxosState paxosState;

    // For sending messages between nodes
    private final MessageRouter router;

    // All participants in the cluster
    private final List<String> allNodeIds;

    // This node's unique ID
    private final String myNodeId;

    private final ConsensusBroadcaster broadcaster;

    // Local counter for generating unique proposal numbers
    private final AtomicInteger proposalCounter = new AtomicInteger(0);

    // The number of nodes needed for majority
    private final int majority;

    // For each proposalNumber we (as Proposer) start, we store ProposerData
    private final ConcurrentHashMap<Integer, ProposerState> proposalStateMap = new ConcurrentHashMap<>();

    // Accept-count for each proposalNumber (Phase 2)
    private final ConcurrentHashMap<Integer, Integer> acceptCountMap = new ConcurrentHashMap<>();

    public PaxosAlgorithm(String myNodeId, List<String> allNodeIds, MessageRouter router) {
        this.myNodeId = myNodeId;
        this.allNodeIds = allNodeIds;
        this.router = router;
        this.paxosState = new PaxosState(myNodeId);
        this.broadcaster = new ConsensusBroadcaster(router, myNodeId);
        this.majority = (allNodeIds.size() / 2) + 1;
    }

    @Override
    public void propose(Object value) {
        int proposalNumber = generateNextProposalNumber();
        // Store proposer data...
        proposalStateMap.put(proposalNumber, new ProposerState(value));
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
        logger.info("Node {} has COMMITTED value: {}", myNodeId, value);

        // Optionally broadcast a final "CHOSEN" or "COMMIT" message 
        // so other nodes can learn the result.
        // broadcastCommit(proposalNumber, value);
    }

    // Called by the VirtualNodeThread when a message arrives
    @Override
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
                // optional for COMMIT, or ignore
                break;
        }
    }

    // ---------------------- Phase 1: Prepare / Promise ----------------------
    private void broadcastPrepareRequest(int proposalNumber, Object originalValue) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(originalValue);
        // Broadcast the same message to all nodes
        broadcaster.broadcast(MessageType.PREPARE_REQUEST, payload);
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

            SimulationMessage promiseMsg = SimulationMessageFactory.createMessage(myNodeId, sourceNode, MessageType.PROMISE, reply);
            router.messageSent(promiseMsg);
        }
        // else: we could send a REJECT message if we want, or just ignore
    }

    private void onPromise(String sourceNodeId, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        ProposerState state = proposalStateMap.get(proposalNumber);
        if (state == null) {
            // Stale or duplicate promise; ignore.
            return;
        }
        // Increment the promise count
        state.incrementPromiseCount();
        // Update the highest accepted proposal if applicable
        state.updateHighestAccepted(payload.getAcceptedId(), payload.getAcceptedValue());

        logger.info("Received PROMISE from {} for proposal #{} (count = {})",
                sourceNodeId, proposalNumber, state.getPromiseCount());

        // If quorum is reached, decide on the value and move to the accept phase.
        if (state.getPromiseCount() >= majority) {
            Object valueToAccept = (state.getHighestAcceptedValue() != null)
                    ? state.getHighestAcceptedValue()
                    : state.getOriginalValue();
            broadcastAcceptRequest(proposalNumber, valueToAccept);
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
            SimulationMessage promiseMsg = SimulationMessageFactory.createMessage(myNodeId, nodeId, MessageType.PROMISE, payload);
            router.messageSent(promiseMsg);
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

            SimulationMessage promiseMsg = SimulationMessageFactory.createMessage(myNodeId, sourceNode, MessageType.PROMISE, acceptedPayload);
            router.messageSent(promiseMsg);
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
}
