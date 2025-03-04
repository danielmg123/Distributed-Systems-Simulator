package com.dss.backend.algorithm.consensus.multi_paxos;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.paxos.PaxosPayload;
import com.dss.backend.algorithm.consensus.paxos.ProposerState;
import com.dss.backend.algorithm.consensus.util.ConsensusBroadcaster;
import com.dss.backend.config.SimulationProperties;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.SimulationMessage;
import com.dss.backend.engine.concurrent.SimulationMessageFactory;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

public class MultiPaxos implements ConsensusAlgorithm {

    private final AppLogger appLogger = new DefaultAppLogger(MultiPaxos.class);

    private final MessageRouter router;
    private final ConsensusBroadcaster broadcaster;
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

    // Use the custom Scheduler instead of a raw executor.
    private final Scheduler scheduler;

    // Timeout for the prepare phase
    private long prepareTimeoutMillis;

    // Simulation properties for configuration
    private final SimulationProperties simulationProperties;

    /**
     * Constructs a MultiPaxos instance with all required dependencies.
     *
     * @param router                The MessageRouter instance used for sending/receiving messages.
     * @param simulationProperties  The SimulationProperties for configuration.
     * @param scheduler             The Scheduler abstraction for scheduling tasks.
     */
    public MultiPaxos(MessageRouter router, SimulationProperties simulationProperties, Scheduler scheduler) {
        this.router = router;
        this.simulationProperties = simulationProperties;
        this.scheduler = scheduler;
        // Assume "self" as the local node id for simplicity.
        this.broadcaster = new ConsensusBroadcaster(router, "self");
        // Initialize the prepare timeout from external configuration.
        this.prepareTimeoutMillis = simulationProperties.getMultipaxosPrepareTimeoutMillis();
    }

    /**
     * Sets the total number of nodes and computes the quorum.
     * If the SimulationProperties provide a non-zero quorum, that value is used;
     * otherwise, the quorum is computed as (totalNodes/2) + 1.
     *
     * @param totalNodes Total number of nodes in the system.
     */
    public void setTotalNodes(int totalNodes) {
        this.totalNodes = totalNodes;
        if (simulationProperties.getMultipaxosQuorum() > 0) {
            this.quorum = simulationProperties.getMultipaxosQuorum();
        } else {
            this.quorum = (totalNodes / 2) + 1;
        }
    }

    /**
     * Sets whether this node is the leader.
     *
     * @param isLeader true if this node is the leader.
     */
    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    @Override
    public void propose(Object value) {
        if (isLeader) {
            if (!preparePhaseCompleted) {
                currentProposalNumber = ++proposalCounter;
                // Initialize the per-proposal state
                proposerState = new ProposerState(value);
                broadcastPrepareRequest(currentProposalNumber);
                // Schedule a timeout to reset the prepare phase if quorum is not reached.
                scheduler.schedule(() -> {
                    if (!preparePhaseCompleted) {
                        resetPreparePhase();
                    }
                }, prepareTimeoutMillis, TimeUnit.MILLISECONDS);
            } else {
                // Fast path: if prepare phase already completed, send accept request directly.
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

    @Override
    public void commit(Object value) {
        committedValue = value;
        appLogger.info("MultiPaxos committed value: {}", value);
        broadcastCommit(currentProposalNumber, value);
    }

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
        payload.setProposedValue(proposerState.getOriginalValue());
        broadcaster.broadcast(MessageType.PREPARE_REQUEST, payload);
    }

    private void broadcastAcceptRequest(int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);
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

    private void onPrepareRequest(String sourceNodeId, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber >= promisedId) {
            promisedId = proposalNumber;
            PaxosPayload response = new PaxosPayload();
            response.setProposalNumber(proposalNumber);
            response.setAcceptedId(acceptedId);
            response.setAcceptedValue(acceptedValue);
            SimulationMessage msg = SimulationMessageFactory.createMessage("self", sourceNodeId, MessageType.PROMISE, response);
            router.messageSent(msg);
            appLogger.info("Sent PROMISE for proposal #{} to {}", proposalNumber, sourceNodeId);
        } else {
            appLogger.info("Ignored PREPARE_REQUEST for proposal #{} because promisedId is {}", proposalNumber, promisedId);
        }
    }

    private void onPromise(String sourceNodeId, PaxosPayload payload) {
        if (!isLeader || preparePhaseCompleted) {
            return;
        }
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber != currentProposalNumber) {
            return;
        }
        proposerState.incrementPromiseCount();
        proposerState.updateHighestAccepted(payload.getAcceptedId(), payload.getAcceptedValue());
        appLogger.info("Received PROMISE from {} for proposal #{} (count = {})",
                sourceNodeId, proposalNumber, proposerState.getPromiseCount());
        if (proposerState.getPromiseCount() >= quorum) {
            Object valueToAccept = (proposerState.getHighestAcceptedValue() != null)
                    ? proposerState.getHighestAcceptedValue()
                    : proposerState.getOriginalValue();
            preparePhaseCompleted = true;
            appLogger.info("Prepare phase complete with quorum reached. Moving to accept phase with value: {}", valueToAccept);
            broadcastAcceptRequest(currentProposalNumber, valueToAccept);
        }
    }

    private void onAcceptRequest(String sourceNodeId, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber >= promisedId) {
            promisedId = proposalNumber;
            acceptedId = proposalNumber;
            acceptedValue = payload.getProposedValue();
            PaxosPayload response = new PaxosPayload();
            response.setProposalNumber(proposalNumber);
            response.setProposedValue(acceptedValue);
            SimulationMessage msg = SimulationMessageFactory.createMessage("self", sourceNodeId, MessageType.ACCEPTED, response);
            router.messageSent(msg);
            appLogger.info("Accepted proposal #{} from {}", proposalNumber, sourceNodeId);
        } else {
            appLogger.info("Rejected ACCEPT_REQUEST for proposal #{} (promisedId = {})", proposalNumber, promisedId);
        }
    }

    private void onAccepted(String sourceNodeId, PaxosPayload payload) {
        if (!isLeader) {
            return;
        }
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber != currentProposalNumber) {
            return;
        }
        acceptResponseCount++;
        appLogger.info("Received ACCEPTED from {} for proposal #{} (count = {})",
                sourceNodeId, proposalNumber, acceptResponseCount);
        if (acceptResponseCount >= quorum) {
            Object valueToCommit = payload.getProposedValue();
            appLogger.info("Quorum reached on ACCEPTED responses. Committing value: {}", valueToCommit);
            commit(valueToCommit);
        }
    }

    private void onCommit(String sourceNodeId, PaxosPayload payload) {
        committedValue = payload.getProposedValue();
        appLogger.info("Node received COMMIT from {} for proposal #{} with value: {}",
                sourceNodeId, payload.getProposalNumber(), committedValue);
    }
}