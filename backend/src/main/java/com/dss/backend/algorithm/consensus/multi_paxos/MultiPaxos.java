package com.dss.backend.algorithm.consensus.multi_paxos;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.algorithm.consensus.paxos.PaxosPayload;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class MultiPaxos implements ConsensusAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(MultiPaxos.class);

    private MessageRouter router;
    private int totalNodes = 1;
    private int quorum = 1;

    private boolean isLeader = false;
    private int proposalCounter = 0;

    // Prepare phase variables
    @Getter
    private boolean preparePhaseCompleted = false;
    @Getter
    private int currentProposalNumber = 0;
    private int preparePromiseCount = 0;
    private Object proposedValueForPrepare;
    private int highestAcceptedId = -1;
    private Object highestAcceptedValue = null;

    // Accept phase variables
    private int acceptResponseCount = 0;

    // Node state
    private int promisedId = -1;
    private int acceptedId = -1;
    private Object acceptedValue = null;
    @Getter
    private Object committedValue = null;

    // Timeout handling
    private long prepareTimeoutMillis = 5000; // 5 seconds
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // --- Setters for dependencies and configuration ---

    public void setMessageRouter(MessageRouter router) {
        this.router = router;
    }

    public void setTotalNodes(int totalNodes) {
        this.totalNodes = totalNodes;
        this.quorum = (totalNodes / 2) + 1;
    }

    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    // --- ConsensusAlgorithm Interface Methods ---

    /**
     * Initiates a proposal. If the node is the leader and has not yet completed the
     * prepare phase, a full prepare phase is executed. Otherwise, the leader takes
     * the fast path (directly broadcasting an ACCEPT_REQUEST).
     *
     * Non-leader nodes log that proposals must be forwarded.
     */
    @Override
    public void propose(Object value) {
        if (isLeader) {
            if (!preparePhaseCompleted) {
                // Begin full prepare phase for this leadership term.
                currentProposalNumber = ++proposalCounter;
                proposedValueForPrepare = value;
                preparePromiseCount = 0;
                highestAcceptedId = -1;
                highestAcceptedValue = null;
                logger.info("Leader starting prepare phase with proposal #{} and proposed value: {}", currentProposalNumber, value);
                broadcastPrepareRequest(currentProposalNumber);
                // Schedule a timeout task for the prepare phase:
                scheduler.schedule(() -> {
                    if (!preparePhaseCompleted) {
                        logger.info("Prepare phase timeout for proposal #{}", currentProposalNumber);
                        // Reset the prepare phase (or you could retry the prepare phase)
                        resetPreparePhase();
                    }
                }, prepareTimeoutMillis, TimeUnit.MILLISECONDS);
            } else {
                // Fast path: prepare phase already done; propose directly.
                currentProposalNumber = ++proposalCounter;
                logger.info("Leader fast-path proposing value '{}' with proposal #{}", value, currentProposalNumber);
                broadcastAcceptRequest(currentProposalNumber, value);
            }
        } else {
            logger.info("Node is not leader. It must forward proposals to the leader.");
        }
    }

    private void resetPreparePhase() {
        preparePhaseCompleted = false;
        preparePromiseCount = 0;
        highestAcceptedId = -1;
        highestAcceptedValue = null;
        logger.info("Resetting prepare phase for proposal #{}", currentProposalNumber);
    }

    /**
     * Accepts a proposal if its proposal number is at least the promised value.
     * Used when processing an ACCEPT_REQUEST.
     */
    @Override
    public boolean accept(Object proposal) {
        PaxosPayload payload = (PaxosPayload) proposal;
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber >= promisedId) {
            promisedId = proposalNumber;
            acceptedId = proposalNumber;
            acceptedValue = payload.getProposedValue();
            logger.info("Node accepted proposal #{} with value: {}", proposalNumber, acceptedValue);
            return true;
        }
        logger.info("Node rejected proposal #{} (promisedId = {})", proposalNumber, promisedId);
        return false;
    }

    /**
     * Commits the provided value and logs the commitment.
     * Also, this method would broadcast a COMMIT message to other nodes.
     */
    @Override
    public void commit(Object value) {
        committedValue = value;
        logger.info("MultiPaxos committed value: {}", value);
        broadcastCommit(currentProposalNumber, value);
        // Reset preparePhase for subsequent proposals if needed.
        // (Depending on the protocol design, we might retain preparePhaseCompleted until leadership changes.)
    }

    /**
     * Handles incoming messages and routes them to appropriate internal handlers.
     */
    @Override
    public void handleMessage(SimulationMessage msg) {
        // Handle messages based on their type.
        MessageType type = msg.getType();
        PaxosPayload payload = (PaxosPayload) msg.getPayload();
        switch (type) {
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
            case COMMIT:
                onCommit(msg.getSourceNodeId(), payload);
                break;
            default:
                logger.warn("MultiPaxos: Unhandled message type: {}", type);
                break;
        }
    }

    // --- Broadcasting Helper Methods ---

    private void broadcastPrepareRequest(int proposalNumber) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(proposedValueForPrepare);
        // broadcast to all nodes
        for (String nodeId : router.getRegisteredNodeIds()) {
            try {
                SimulationMessage msg = new SimulationMessage("self", nodeId, MessageType.PREPARE_REQUEST, payload);
                router.messageSent(msg);
            } catch (Exception e) {
                logger.error("Error broadcasting PREPARE_REQUEST to {}: {}", nodeId, e.getMessage());
            }
        }
    }

    private void broadcastAcceptRequest(int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);
        // reset accept count for proposal
        acceptResponseCount = 0;
        for (String nodeId : router.getRegisteredNodeIds()) {
            try {
                SimulationMessage msg = new SimulationMessage("self", nodeId, MessageType.ACCEPT_REQUEST, payload);
                router.messageSent(msg);
            } catch (Exception e) {
                logger.error("Error broadcasting ACCEPT_REQUEST to {}: {}", nodeId, e.getMessage());
            }
        }
    }

    private void broadcastCommit(int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);

        for (String nodeId : router.getRegisteredNodeIds()) {
            try {
                SimulationMessage msg = new SimulationMessage("self", nodeId, MessageType.COMMIT, payload);
                router.messageSent(msg);
            } catch (Exception e) {
                logger.error("Error broadcasting COMMIT to {}: {}", nodeId, e.getMessage());
            }
        }
    }

    // --- Message Handlers ---

    /**
     * Handler for incoming PREPARE_REQUEST messages.
     * As an acceptor, if the proposal number is high enough, update promisedId and reply with a PROMISE.
     */
    private void onPrepareRequest(String sourceNodeId, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber >= promisedId) {
            promisedId = proposalNumber;
            // Prepare response with our current accepted proposal (if any)
            PaxosPayload response = new PaxosPayload();
            response.setProposalNumber(proposalNumber);
            response.setAcceptedId(acceptedId);
            response.setAcceptedValue(acceptedValue);
            SimulationMessage promiseMsg = new SimulationMessage("self", sourceNodeId, MessageType.PROMISE, response);
            router.messageSent(promiseMsg);
            logger.info("Sent PROMISE for proposal #{} to {}", proposalNumber, sourceNodeId);
        } else {
            logger.info("Ignored PREPARE_REQUEST for proposal #{} because promisedId is {}", proposalNumber, promisedId);
        }
    }

    /**
     * Handler for incoming PROMISE messages.
     * Only the leader (proposer) processes these during its prepare phase.
     * When a quorum is reached, the leader selects the value to propose (either its own or the highest accepted)
     * and moves to the accept phase.
     */
    private void onPromise(String sourceNodeId, PaxosPayload payload) {
        // Only process if we are the leader and in prepare phase.
        if (!isLeader || preparePhaseCompleted) {
            return;
        }
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber != currentProposalNumber) {
            return; // Stale promise.
        }
        preparePromiseCount++;
        // If the responder had already accepted a proposal, use the highest one.
        if (payload.getAcceptedId() > highestAcceptedId && payload.getAcceptedValue() != null) {
            highestAcceptedId = payload.getAcceptedId();
            highestAcceptedValue = payload.getAcceptedValue();
        }
        logger.info("Received PROMISE from {} for proposal #{} (count = {})", sourceNodeId, proposalNumber, preparePromiseCount);
        if (preparePromiseCount >= quorum) {
            // Decide on the value: if any node had already accepted a proposal, adopt that.
            Object valueToAccept = (highestAcceptedValue != null) ? highestAcceptedValue : proposedValueForPrepare;
            preparePhaseCompleted = true;
            logger.info("Prepare phase complete with quorum reached. Moving to accept phase with value: {}", valueToAccept);
            broadcastAcceptRequest(currentProposalNumber, valueToAccept);
        }
    }

    /**
     * Handler for incoming ACCEPT_REQUEST messages.
     * As an acceptor, accept the proposal if its proposal number is not less than our promisedId.
     */
    private void onAcceptRequest(String sourceNodeId, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber >= promisedId) {
            promisedId = proposalNumber;
            acceptedId = proposalNumber;
            acceptedValue = payload.getProposedValue();
            // Send ACCEPTED message back to proposer.
            PaxosPayload response = new PaxosPayload();
            response.setProposalNumber(proposalNumber);
            response.setProposedValue(acceptedValue);
            SimulationMessage acceptedMsg = new SimulationMessage("self", sourceNodeId, MessageType.ACCEPTED, response);
            router.messageSent(acceptedMsg);
            logger.info("Accepted proposal #{} from {}", proposalNumber, sourceNodeId);
        } else {
            logger.info("Rejected ACCEPT_REQUEST for proposal #{} (promisedId = {})", proposalNumber, promisedId);
        }
    }

    /**
     * Handler for incoming ACCEPTED messages.
     * The leader (proposer) counts ACCEPTED responses, and once a quorum is reached,
     * commits the value.
     */
    private void onAccepted(String sourceNodeId, PaxosPayload payload) {
        if (!isLeader) {
            return;
        }
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber != currentProposalNumber) {
            return; // Ignore responses for old proposals.
        }
        acceptResponseCount++;
        logger.info("Received ACCEPTED from {} for proposal #{} (count = {})", sourceNodeId, proposalNumber, acceptResponseCount);
        if (acceptResponseCount >= quorum) {
            Object valueToCommit = payload.getProposedValue();
            logger.info("Quorum reached on ACCEPTED responses. Committing value: {}", valueToCommit);
            commit(valueToCommit);
        }
    }

    /**
     * Handler for COMMIT messages.
     * Upon receiving a COMMIT, update local committed value.
     */
    private void onCommit(String sourceNodeId, PaxosPayload payload) {
        committedValue = payload.getProposedValue();
        logger.info("Node received COMMIT from {} for proposal #{} with value: {}", sourceNodeId, payload.getProposalNumber(), committedValue);
        // Optionally, reset preparePhaseCompleted for the next proposal if needed.
    }
}
