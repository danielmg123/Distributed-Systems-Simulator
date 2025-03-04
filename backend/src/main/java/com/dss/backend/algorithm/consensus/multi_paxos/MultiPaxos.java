package com.dss.backend.algorithm.consensus.multi_paxos;

import com.dss.backend.algorithm.consensus.paxos.ProposerState;
import com.dss.backend.algorithm.consensus.util.ConsensusBroadcaster;
import com.dss.backend.engine.concurrent.SimulationMessageFactory;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.algorithm.consensus.paxos.PaxosPayload;
import com.dss.backend.config.SimulationProperties;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class MultiPaxos implements ConsensusAlgorithm {

    private final AppLogger appLogger = new DefaultAppLogger(MultiPaxos.class);

    private MessageRouter router;
    private ConsensusBroadcaster broadcaster;
    private int totalNodes = 1;
    private int quorum = 1;

    private boolean isLeader = false;
    private int proposalCounter = 0;

    private ProposerState proposerState;

    // Prepare phase variables
    @Getter
    private boolean preparePhaseCompleted = false;
    @Getter
    private int currentProposalNumber = 0;

    // Accept phase variables
    private int acceptResponseCount = 0;

    // Node state
    private int promisedId = -1;
    private int acceptedId = -1;
    private Object acceptedValue = null;
    @Getter
    private Object committedValue = null;

    @Setter
    private ScheduledExecutorService scheduler;

    // Add a setter to allow overriding the prepare timeout (default is 5000 ms)
    // Timeout handling
    @Setter
    private long prepareTimeoutMillis;

    // Inject SimulationProperties to externalize configuration:
    @Autowired
    private SimulationProperties simulationProperties;

    @PostConstruct
    public void init() {
        this.prepareTimeoutMillis = simulationProperties.getMultipaxosPrepareTimeoutMillis();
    }

    // --- Setters for dependencies and configuration ---

    public void setMessageRouter(MessageRouter router) {
        this.router = router;
        // For simplicity, assume "self" as the local node id. In future version might inject that too.
        this.broadcaster = new ConsensusBroadcaster(router, "self");
    }

    // Setter for totalNodes updated to use externalized quorum if set
    public void setTotalNodes(int totalNodes) {
        this.totalNodes = totalNodes;
        if (simulationProperties != null && simulationProperties.getMultipaxosQuorum() > 0) {
            this.quorum = simulationProperties.getMultipaxosQuorum();
        } else {
            this.quorum = (totalNodes / 2) + 1;  // Default to majority quorum
        }
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
                currentProposalNumber = ++proposalCounter;
                // Initialize the per-proposal state
                proposerState = new ProposerState(value);
                broadcastPrepareRequest(currentProposalNumber);
                // Use the scheduler for timeout (unchanged)
                scheduler.schedule(() -> {
                    if (!preparePhaseCompleted) {
                        resetPreparePhase();
                    }
                }, prepareTimeoutMillis, TimeUnit.MILLISECONDS);
            } else {
                // Fast path for subsequent proposals:
                currentProposalNumber = ++proposalCounter;
                broadcastAcceptRequest(currentProposalNumber, value);
            }
        } else {
            appLogger.info("Node is not leader. It must forward proposals to the leader.");
        }
    }

    private void resetPreparePhase() {
        preparePhaseCompleted = false;
        proposerState = null;
        appLogger.info("Resetting prepare phase for proposal #{}", currentProposalNumber);
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
            appLogger.info("Node accepted proposal #{} with value: {}", proposalNumber, acceptedValue);
            return true;
        }
        appLogger.info("Node rejected proposal #{} (promisedId = {})", proposalNumber, promisedId);
        return false;
    }

    /**
     * Commits the provided value and logs the commitment.
     * Also, this method would broadcast a COMMIT message to other nodes.
     */
    @Override
    public void commit(Object value) {
        committedValue = value;
        appLogger.info("MultiPaxos committed value: {}", value);
        broadcastCommit(currentProposalNumber, value);
    }

    /**
     * Handles incoming messages and routes them to appropriate internal handlers.
     */
    @Override
    public void handleMessage(SimulationMessage msg) {
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
                appLogger.debug("MultiPaxos: Unhandled message type: {}", type);
                break;
        }
    }

    // --- Broadcasting Helper Methods ---

    private void broadcastPrepareRequest(int proposalNumber) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        // Use the original proposed value from the proposer state
        payload.setProposedValue(proposerState.getOriginalValue());
        broadcaster.broadcast(MessageType.PREPARE_REQUEST, payload);
    }


    private void broadcastAcceptRequest(int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);
        // reset accept count for proposal
        acceptResponseCount = 0;
        broadcaster.broadcast(MessageType.ACCEPT_REQUEST, payload);
    }

    private void broadcastCommit(int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);
        broadcaster.broadcast(MessageType.COMMIT, payload);
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
            SimulationMessage msg = SimulationMessageFactory.createMessage("self", sourceNodeId, MessageType.PROMISE, payload);
            router.messageSent(msg);

            appLogger.info("Sent PROMISE for proposal #{} to {}", proposalNumber, sourceNodeId);
        } else {
            appLogger.info("Ignored PREPARE_REQUEST for proposal #{} because promisedId is {}", proposalNumber, promisedId);
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
        proposerState.incrementPromiseCount();
        proposerState.updateHighestAccepted(payload.getAcceptedId(), payload.getAcceptedValue());
        appLogger.info("Received PROMISE from {} for proposal #{} (count = {})", sourceNodeId, proposalNumber, proposerState.getPromiseCount());
        if (proposerState.getPromiseCount() >= quorum) {
            Object valueToAccept = (proposerState.getHighestAcceptedValue() != null)
                    ? proposerState.getHighestAcceptedValue()
                    : proposerState.getOriginalValue();
            preparePhaseCompleted = true;
            appLogger.info("Prepare phase complete with quorum reached. Moving to accept phase with value: {}", valueToAccept);
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
            SimulationMessage msg = SimulationMessageFactory.createMessage("self", sourceNodeId, MessageType.ACCEPTED, payload);
            router.messageSent(msg);
            appLogger.info("Accepted proposal #{} from {}", proposalNumber, sourceNodeId);
        } else {
            appLogger.info("Rejected ACCEPT_REQUEST for proposal #{} (promisedId = {})", proposalNumber, promisedId);
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
        appLogger.info("Received ACCEPTED from {} for proposal #{} (count = {})", sourceNodeId, proposalNumber, acceptResponseCount);
        if (acceptResponseCount >= quorum) {
            Object valueToCommit = payload.getProposedValue();
            appLogger.info("Quorum reached on ACCEPTED responses. Committing value: {}", valueToCommit);
            commit(valueToCommit);
        }
    }

    /**
     * Handler for COMMIT messages.
     * Upon receiving a COMMIT, update local committed value.
     */
    private void onCommit(String sourceNodeId, PaxosPayload payload) {
        committedValue = payload.getProposedValue();
        appLogger.info("Node received COMMIT from {} for proposal #{} with value: {}", sourceNodeId, payload.getProposalNumber(), committedValue);
        // Optionally, reset preparePhaseCompleted for the next proposal if needed.
    }
}
