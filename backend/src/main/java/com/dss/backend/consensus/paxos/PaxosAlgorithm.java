package com.dss.backend.consensus.paxos;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.dss.backend.consensus.AbstractConsensusAlgorithm;
import com.dss.backend.consensus.util.ConsensusBroadcaster;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.SimulationMessageFactory;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;

/**
 * PaxosAlgorithm implements the Paxos consensus protocol. It merges information from
 * all PROMISE messages and uses the highest accepted proposal when moving to the accept phase.
 *
 * Refactored to extend AbstractConsensusAlgorithm so that default no-op implementations for methods
 * (such as accept() or commit() where appropriate) are inherited.
 *
 * Also, the message types used for responses have been corrected for consistency;
 * for example, in the accept phase we now send a message with type ACCEPTED.
 */
public class PaxosAlgorithm extends AbstractConsensusAlgorithm {

    private final AppLogger appLogger = new DefaultAppLogger(PaxosAlgorithm.class);

    // Local Paxos state for this node.
    private final PaxosState paxosState;

    // Message router for sending messages between nodes.
    private final MessageRouter router;

    // List of all participating node IDs.
    private final List<String> allNodeIds;

    // This node's unique ID.
    private final String myNodeId;

    // Helper for broadcasting messages (excludes sending to self).
    private final ConsensusBroadcaster broadcaster;

    // Local counter for generating unique proposal numbers.
    private final AtomicInteger proposalCounter = new AtomicInteger(0);

    // Majority count needed for quorum.
    private final int majority;

    // Map of proposal number to its associated proposer state.
    private final ConcurrentHashMap<Integer, ProposerState> proposalStateMap = new ConcurrentHashMap<>();

    // Map for counting ACCEPTED responses for each proposal.
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
        // Generate a new proposal number and create a new proposer state.
        int proposalNumber = generateNextProposalNumber();
        proposalStateMap.put(proposalNumber, new ProposerState(value));
        broadcastPrepareRequest(proposalNumber, value);
    }

    @Override
    public boolean accept(Object proposal) {
        // Not directly used in Paxos – acceptance is handled via message processing.
        return false;
    }

    @Override
    public void commit(Object value) {
        // Mark the chosen value in our Paxos state.
        paxosState.setChosenValue(value);
        appLogger.info("Node {} has COMMITTED value: {}", myNodeId, value);
    }

    @Override
    public void handleMessage(SimulationMessage msg) {
        // Only process messages whose payload is an instance of PaxosPayload.
        if (!(msg.getPayload() instanceof PaxosPayload)) {
            return;
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
            // Additional message types (e.g., COMMIT) could be handled here.
            default:
                // Ignore unhandled message types.
                break;
        }
    }

    // ---------------------- Phase 1: Prepare / Promise ----------------------

    /**
     * Broadcasts a PREPARE_REQUEST message to all nodes.
     */
    private void broadcastPrepareRequest(int proposalNumber, Object originalValue) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(originalValue);
        broadcaster.broadcast(MessageType.PREPARE_REQUEST, payload);
    }

    /**
     * Handles an incoming PREPARE_REQUEST.
     * If the proposal number is greater than or equal to our promised ID, we promise to accept
     * no proposals with a lower number.
     */
    private void onPrepareRequest(String sourceNode, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber >= paxosState.getPromisedId()) {
            paxosState.setPromisedId(proposalNumber);

            // Prepare a PROMISE response that includes any previously accepted proposal.
            PaxosPayload reply = new PaxosPayload();
            reply.setProposalNumber(proposalNumber);
            reply.setAcceptedId(paxosState.getAcceptedId());
            reply.setAcceptedValue(paxosState.getAcceptedValue());

            SimulationMessage promiseMsg = SimulationMessageFactory.createMessage(
                    myNodeId, sourceNode, MessageType.PROMISE, reply);
            router.messageSent(promiseMsg);
        }
        // Optionally, else send a rejection.
    }

    /**
     * Processes an incoming PROMISE message.
     * Once a quorum is reached, proceeds to the accept phase.
     */
    private void onPromise(String sourceNodeId, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        ProposerState state = proposalStateMap.get(proposalNumber);
        if (state == null) {
            // Ignore stale or duplicate promises.
            return;
        }
        state.incrementPromiseCount();
        state.updateHighestAccepted(payload.getAcceptedId(), payload.getAcceptedValue());

        appLogger.info("Received PROMISE from {} for proposal #{} (count = {})",
                sourceNodeId, proposalNumber, state.getPromiseCount());

        // Once quorum is reached, decide which value to propose.
        if (state.getPromiseCount() >= majority) {
            Object valueToAccept = (state.getHighestAcceptedValue() != null)
                    ? state.getHighestAcceptedValue()
                    : state.getOriginalValue();
            broadcastAcceptRequest(proposalNumber, valueToAccept);
        }
    }

    // ---------------------- Phase 2: Accept / Accepted ----------------------

    /**
     * Broadcasts an ACCEPT_REQUEST message to all nodes.
     */
    private void broadcastAcceptRequest(int proposalNumber, Object value) {
        // Reset the count for ACCEPTED responses.
        acceptCountMap.put(proposalNumber, 0);

        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);

        // Broadcast ACCEPT_REQUEST to all nodes.
        for (String nodeId : allNodeIds) {
            SimulationMessage acceptRequestMsg = SimulationMessageFactory.createMessage(
                    myNodeId, nodeId, MessageType.ACCEPT_REQUEST, payload);
            router.messageSent(acceptRequestMsg);
        }
    }

    /**
     * Handles an incoming ACCEPT_REQUEST.
     * If the proposal number is acceptable, the node updates its state and sends back an ACCEPTED message.
     */
    private void onAcceptRequest(String sourceNode, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber >= paxosState.getPromisedId()) {
            paxosState.setAcceptedId(proposalNumber);
            paxosState.setAcceptedValue(payload.getProposedValue());

            // Create an ACCEPTED response with the correct message type.
            PaxosPayload acceptedPayload = new PaxosPayload();
            acceptedPayload.setProposalNumber(proposalNumber);
            acceptedPayload.setProposedValue(payload.getProposedValue());

            SimulationMessage acceptedMsg = SimulationMessageFactory.createMessage(
                    myNodeId, sourceNode, MessageType.ACCEPTED, acceptedPayload);
            router.messageSent(acceptedMsg);
            appLogger.info("Accepted proposal #{} from {}", proposalNumber, sourceNode);
        } else {
            appLogger.info("Rejected ACCEPT_REQUEST for proposal #{} (promisedId = {})", proposalNumber, paxosState.getPromisedId());
        }
    }

    /**
     * Processes an incoming ACCEPTED message.
     * When a quorum of ACCEPTED messages is reached, the proposal is committed.
     */
    private void onAccepted(String sourceNode, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        Integer oldCount = acceptCountMap.get(proposalNumber);
        if (oldCount == null) {
            // Ignore stale or unexpected responses.
            return;
        }
        int newCount = oldCount + 1;
        acceptCountMap.put(proposalNumber, newCount);

        // If a majority of nodes have accepted the proposal, commit the value.
        if (newCount >= majority) {
            commit(payload.getProposedValue());
            // Optionally clean up data structures related to the proposal.
        }
    }

    // ---------- Helper: Generate Unique Proposal IDs ----------

    /**
     * Generates a unique proposal number by incrementing a local counter.
     */
    private int generateNextProposalNumber() {
        return proposalCounter.incrementAndGet();
    }
}