package com.dss.backend.algorithm.consensus.multi_paxos;

import lombok.Getter;
import org.springframework.stereotype.Component;
import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.algorithm.consensus.paxos.PaxosPayload;

/**
 * This implementation supports:
 *   - A full prepare/promise phase for the first proposal (or when a new leadership term begins)
 *   - A fast path for subsequent proposals once the prepare phase has been completed.
 *   - Quorum-based decision making by counting PROMISE and ACCEPTED responses.
 *
 * NTS:
 *   - Leader election is simulated via an externally set flag (isLeader).
 *   - Non-leader nodes simply respond to prepare/accept requests.
 *   - In a real system, we would forward proposals from non-leaders to the leader.
 */
@Component
public class MultiPaxos implements ConsensusAlgorithm {

    // Injected or set externally via setters
    private MessageRouter router;
    private int totalNodes = 1; // Total number of nodes in the simulation
    private int quorum = 1;     // Computed as (totalNodes/2)+1

    // Leader flag (simulate leader election externally)
    private boolean isLeader = false;

    // Global proposal counter (increases monotonically)
    private int proposalCounter = 0;

    // For the prepare phase
    @Getter
    private boolean preparePhaseCompleted = false;
    @Getter
    private int currentProposalNumber = 0;
    private int preparePromiseCount = 0;
    private Object proposedValueForPrepare;
    private int highestAcceptedId = -1;
    private Object highestAcceptedValue = null;

    // For the accept phase
    private int acceptResponseCount = 0;

    // Local state maintained for each node (for when acting as an acceptor)
    private int promisedId = -1;
    private int acceptedId = -1;
    private Object acceptedValue = null;
    @Getter
    private Object committedValue = null;

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
                System.out.println("Leader starting prepare phase with proposal #" + currentProposalNumber +
                        " and proposed value: " + value);
                broadcastPrepareRequest(currentProposalNumber);
            } else {
                // Fast path: prepare phase already done; propose directly.
                currentProposalNumber = ++proposalCounter;
                System.out.println("Leader fast-path proposing value '" + value +
                        "' with proposal #" + currentProposalNumber);
                broadcastAcceptRequest(currentProposalNumber, value);
            }
        } else {
            System.out.println("Node is not leader. It must forward proposals to the leader.");
        }
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
            System.out.println("Node accepted proposal #" + proposalNumber +
                    " with value: " + acceptedValue);
            return true;
        }
        System.out.println("Node rejected proposal #" + proposalNumber +
                " (promisedId = " + promisedId + ")");
        return false;
    }

    /**
     * Commits the provided value and logs the commitment.
     * Also, this method would broadcast a COMMIT message to other nodes.
     */
    @Override
    public void commit(Object value) {
        committedValue = value;
        System.out.println("MultiPaxos committed value: " + value);
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
                System.out.println("MultiPaxos: Unhandled message type: " + type);
                break;
        }
    }

    // --- Broadcasting Helper Methods ---

    private void broadcastPrepareRequest(int proposalNumber) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(proposedValueForPrepare);
        // Broadcast to all nodes.
        for (String nodeId : router.getRegisteredNodeIds()) {
            SimulationMessage msg = new SimulationMessage(
                    /*sourceNodeId=*/ "self", // replace with local node ID if available
                    nodeId,
                    MessageType.PREPARE_REQUEST,
                    payload
            );
            router.messageSent(msg);
        }
    }

    private void broadcastAcceptRequest(int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);
        // Reset accept count for this proposal.
        acceptResponseCount = 0;
        for (String nodeId : router.getRegisteredNodeIds()) {
            SimulationMessage msg = new SimulationMessage(
                    "self",
                    nodeId,
                    MessageType.ACCEPT_REQUEST,
                    payload
            );
            router.messageSent(msg);
        }
    }

    private void broadcastCommit(int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);
        for (String nodeId : router.getRegisteredNodeIds()) {
            SimulationMessage msg = new SimulationMessage(
                    "self",
                    nodeId,
                    MessageType.COMMIT,
                    payload
            );
            router.messageSent(msg);
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
            System.out.println("Sent PROMISE for proposal #" + proposalNumber + " to " + sourceNodeId);
        } else {
            System.out.println("Ignored PREPARE_REQUEST for proposal #" + proposalNumber +
                    " because promisedId is " + promisedId);
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
        System.out.println("Received PROMISE from " + sourceNodeId + " for proposal #" + proposalNumber +
                " (count = " + preparePromiseCount + ")");
        if (preparePromiseCount >= quorum) {
            // Decide on the value: if any node had already accepted a proposal, adopt that.
            Object valueToAccept = (highestAcceptedValue != null) ? highestAcceptedValue : proposedValueForPrepare;
            preparePhaseCompleted = true;
            System.out.println("Prepare phase complete with quorum reached. Moving to accept phase with value: " + valueToAccept);
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
            System.out.println("Accepted proposal #" + proposalNumber + " from " + sourceNodeId);
        } else {
            System.out.println("Rejected ACCEPT_REQUEST for proposal #" + proposalNumber +
                    " (promisedId = " + promisedId + ")");
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
        System.out.println("Received ACCEPTED from " + sourceNodeId + " for proposal #" + proposalNumber +
                " (count = " + acceptResponseCount + ")");
        if (acceptResponseCount >= quorum) {
            Object valueToCommit = payload.getProposedValue();
            System.out.println("Quorum reached on ACCEPTED responses. Committing value: " + valueToCommit);
            commit(valueToCommit);
        }
    }

    /**
     * Handler for COMMIT messages.
     * Upon receiving a COMMIT, update local committed value.
     */
    private void onCommit(String sourceNodeId, PaxosPayload payload) {
        committedValue = payload.getProposedValue();
        System.out.println("Node received COMMIT from " + sourceNodeId + " for proposal #" +
                payload.getProposalNumber() + " with value: " + committedValue);
        // Optionally, reset preparePhaseCompleted for the next proposal if needed.
    }
}
