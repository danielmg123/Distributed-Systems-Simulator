package com.dss.backend.consensus.multi_paxos;

import com.dss.backend.consensus.ConsensusAlgorithm;
import com.dss.backend.consensus.paxos.PaxosMessagingUtil;
import com.dss.backend.consensus.paxos.PaxosPayload;
import com.dss.backend.consensus.paxos.ProposerState;
import com.dss.backend.consensus.util.ConsensusBroadcaster;
import com.dss.backend.config.SimulationProperties;
import com.dss.backend.consensus.util.ConsensusUtils;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import com.dss.backend.messaging.*;
import lombok.Getter;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Implements the Multi-Paxos consensus protocol, an optimization of basic Paxos
 * designed for long-lived leaders and repeated command proposals.
 * <p>
 * <strong>Approach:</strong>
 * <ul>
 *   <li>This implementation divides the protocol into a <em>prepare phase</em> (once per leader)
 *       and an <em>accept phase</em> (repeated for subsequent proposals).</li>
 *   <li>When a node becomes leader, it sends a "prepare request" to gather information
 *       on the highest accepted proposal from other nodes. Once it obtains a quorum
 *       of PROMISE responses, it knows a safe starting proposal number and can directly
 *       move to the accept phase for subsequent values.</li>
 *   <li>If the leader does not receive enough PROMISE responses within a configurable timeout,
 *       it resets the prepare phase and may eventually retry.</li>
 * </ul>
 * <p>
 * <strong>Key Design Decisions:</strong>
 * <ul>
 *   <li>Leader Selection: For simplicity, this codebase designates the first node as leader,
 *       or sets the leader externally (e.g., {@link #setLeader(boolean)}). A real Multi-Paxos
 *       solution would incorporate an additional leader election mechanism.</li>
 *   <li>Timeout-based Retries: If a quorum does not respond to the prepare phase before
 *       {@code prepareTimeoutMillis} elapses, we reset and can re-try.
 *       This keeps the protocol from hanging indefinitely.</li>
 *   <li>Quorum Calculation: By default, the quorum is (totalNodes / 2) + 1 unless overridden
 *       by {@code simulation.multipaxosQuorum} in properties.</li>
 * </ul>
 * <p>
 * <strong>Known Limitations:</strong>
 * <ul>
 *   <li>No built-in mechanism for dynamic leader re-election if the leader fails.</li>
 *   <li>No advanced conflict resolution if multiple nodes mistakenly consider themselves leader
 *       at the same time.</li>
 *   <li>Network partitioning is not fully handled; this code does not attempt to reconcile
 *       diverging proposals if the cluster splits for an extended period.</li>
 * </ul>
 *
 * @author Daniel Morales
 */
public class MultiPaxos implements ConsensusAlgorithm {

    private final AppLogger appLogger = new DefaultAppLogger(MultiPaxos.class);

    private final MessageRouter router;
    private final ConsensusBroadcaster broadcaster;

    /** The total number of nodes in the cluster. */
    private int totalNodes = 1;
    /** The minimum number of responses required for a quorum. */
    private int quorum = 1;

    /** Whether this node is currently acting as the Multi-Paxos leader. */
    private boolean isLeader = false;

    /** Local counter to generate strictly increasing proposal numbers. */
    private int proposalCounter = 0;

    /** Maintains proposal-related state (e.g., highest accepted from others). */
    private ProposerState proposerState;

    // --- Prepare phase variables ---
    @Getter
    private boolean preparePhaseCompleted = false;

    @Getter
    private int currentProposalNumber = 0;

    /** Timer task that times out if we do not receive enough PROMISE messages. */
    private ScheduledFuture<?> prepareTimeoutTask;

    // --- Accept phase variables ---
    private int acceptResponseCount = 0;

    // --- Node-specific Paxos state ---
    private int promisedId = -1;        // The highest proposal ID we promised not to accept below
    private int acceptedId = -1;        // The highest proposal ID we've accepted
    private Object acceptedValue = null; // The value associated with 'acceptedId'

    @Getter
    private Object committedValue = null; // The value that has been committed (chosen)

    /** Scheduler for timeouts and asynchronous tasks (prepare, accept, etc.). */
    private final Scheduler scheduler;

    /** The max time (ms) to wait for PROMISE responses during the prepare phase. */
    private long prepareTimeoutMillis;

    /** External configuration (thread pool size, custom quorum, etc.). */
    private final SimulationProperties simulationProperties;

    /** Unique ID of this node; used for routing messages and identifying log output. */
    private final String localNodeId;

    /**
     * Constructs a MultiPaxos instance with all required dependencies.
     *
     * @param localNodeId          the local node's ID
     * @param router               message router for sending/receiving messages
     * @param simulationProperties system-wide simulation properties
     * @param scheduler            scheduler for time-based tasks (timeouts, repeated checks)
     */
    public MultiPaxos(String localNodeId,
                      MessageRouter router,
                      SimulationProperties simulationProperties,
                      Scheduler scheduler) {
        this.localNodeId = localNodeId;
        this.router = router;
        this.simulationProperties = simulationProperties;
        this.scheduler = scheduler;
        // Helper for broadcasting messages to all other nodes
        this.broadcaster = new ConsensusBroadcaster(router, localNodeId);
        // Fetch the prepare-phase timeout from config
        this.prepareTimeoutMillis = simulationProperties.getMultipaxosPrepareTimeoutMillis();
    }

    /**
     * Sets the total number of nodes and computes the quorum size.
     * <p>
     * If {@code simulation.multipaxosQuorum} > 0, that overrides the normal majority
     * calculation of {@code (totalNodes / 2) + 1}.
     *
     * @param totalNodes total number of nodes in the system
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
     * Assigns this node as leader or follower.
     * In a real system, a separate leadership election protocol would manage this.
     *
     * @param isLeader true if this node is leader
     */
    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    /**
     * Initiates a proposal from the leader side.
     * <p>
     * If the prepare phase is not yet completed, we run prepare first. Otherwise,
     * we skip straight to the accept phase for new proposals.
     *
     * @param value the value/command to propose in the cluster
     */
    @Override
    public void propose(Object value) {
        // Only the leader should initiate proposals.
        if (isLeader) {
            // If we haven't completed the prepare phase, do that first.
            if (!preparePhaseCompleted) {
                currentProposalNumber = ++proposalCounter; // move to next proposal number
                proposerState = new ProposerState(value);

                // Send PREPARE_REQUEST to gather highest accepted proposals from other nodes
                PaxosMessagingUtil.broadcastPrepareRequest(broadcaster,
                        currentProposalNumber,
                        proposerState.getOriginalValue());

                // Schedule a timeout in case we never receive enough PROMISE responses
                prepareTimeoutTask = scheduler.schedule(() -> {
                    if (!preparePhaseCompleted) {
                        resetPreparePhase();
                    }
                }, prepareTimeoutMillis, TimeUnit.MILLISECONDS);

            } else {
                // Prepare phase is done, we can jump directly to accept requests
                currentProposalNumber = ++proposalCounter;
                PaxosMessagingUtil.broadcastAcceptRequest(broadcaster, currentProposalNumber, value);
            }
        } else {
            appLogger.info("Node {} is not leader. It must forward proposals to the leader.", localNodeId);
        }
    }

    /**
     * Resets the prepare phase so we can attempt it again
     * (e.g., if we did not get enough PROMISE responses in time).
     */
    private void resetPreparePhase() {
        preparePhaseCompleted = false;
        proposerState = null;
        appLogger.info("Resetting prepare phase for proposal #{}", currentProposalNumber);
    }

    /**
     * Accept method for incoming accept requests from a leader.
     * <p>
     * If the proposal’s number is >= the highest we’ve promised, we accept it.
     * Otherwise, we reject.
     *
     * @param proposal the PaxosPayload describing the proposal number and value
     * @return true if accepted, false if rejected
     */
    @Override
    public boolean accept(Object proposal) {
        PaxosPayload payload = (PaxosPayload) proposal;
        int proposalNumber = payload.getProposalNumber();

        if (proposalNumber >= promisedId) {
            // We promise not to accept proposals below proposalNumber
            promisedId = proposalNumber;
            acceptedId = proposalNumber;
            acceptedValue = payload.getProposedValue();
            appLogger.info("Node {} accepted proposal #{} with value: {}", localNodeId, proposalNumber, acceptedValue);
            return true;
        }
        // Rejected because we previously promised something higher
        appLogger.info("Node {} rejected proposal #{} (promisedId = {})", localNodeId, proposalNumber, promisedId);
        return false;
    }

    /**
     * Once a proposal obtains a quorum of ACCEPTED responses, the leader commits the value
     * and broadcasts a COMMIT message.
     *
     * @param value the chosen/committed value
     */
    @Override
    public void commit(Object value) {
        committedValue = value;
        appLogger.info("MultiPaxos (node {}) committed value: {}", localNodeId, value);
        broadcastCommit(currentProposalNumber, value);
    }

    /**
     * Processes incoming Paxos-related messages (PREPARE_REQUEST, PROMISE, ACCEPT_REQUEST, ACCEPTED, COMMIT).
     *
     * @param msg the incoming SimulationMessage
     */
    @Override
    public void handleMessage(SimulationMessage msg) {
        PaxosPayload payload = ConsensusUtils.safeCastPayload(msg, PaxosPayload.class);
        if (payload == null) {
            return;
        }
        MessageType type = msg.getType();
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
                appLogger.debug("MultiPaxos (node {}): Unhandled message type: {}", localNodeId, type);
                break;
        }
    }

    // ------------------------------------------------------------------
    //                  Helper Methods for Broadcasting
    // ------------------------------------------------------------------

    /**
     * Broadcasts an ACCEPT_REQUEST to all nodes (after successful prepare).
     */
    private void broadcastAcceptRequest(int proposalNumber, Object value) {
        // reset accept response count for each new proposal
        acceptResponseCount = 0;

        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);

        broadcaster.broadcast(MessageType.ACCEPT_REQUEST, payload, ProtocolType.MULTI_PAXOS);
    }

    /**
     * Broadcasts a COMMIT message after the leader has received enough ACCEPTED responses.
     */
    private void broadcastCommit(int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);

        broadcaster.broadcast(MessageType.COMMIT, payload, ProtocolType.MULTI_PAXOS);
    }

    // ------------------------------------------------------------------
    //                     Message Handlers
    // ------------------------------------------------------------------

    /**
     * Handles an incoming PREPARE_REQUEST from another node.
     * <p>
     * If {@code proposalNumber >= promisedId}, we send back a PROMISE message
     * containing our current accepted proposal info. Otherwise, we ignore.
     */
    private void onPrepareRequest(String sourceNodeId, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber >= promisedId) {
            // Record the new promise to not accept proposals below 'proposalNumber'
            promisedId = proposalNumber;

            // Respond with a PROMISE, including any previously accepted proposal
            PaxosPayload response = new PaxosPayload();
            response.setProposalNumber(proposalNumber);
            response.setAcceptedId(acceptedId);
            response.setAcceptedValue(acceptedValue);

            SimulationMessage promiseMsg = SimulationMessageFactory.createMessage(
                    localNodeId,
                    sourceNodeId,
                    MessageType.PROMISE,
                    response,
                    ProtocolType.MULTI_PAXOS
            );
            router.messageSent(promiseMsg);

            appLogger.info("Node {} sent PROMISE for proposal #{} to {}", localNodeId, proposalNumber, sourceNodeId);
        } else {
            appLogger.info("Node {} ignored PREPARE_REQUEST for proposal #{} because promisedId is {}",
                    localNodeId, proposalNumber, promisedId);
        }
    }

    /**
     * Handles PROMISE responses on the leader node. If we've gathered enough PROMISEs,
     * we complete the prepare phase and proceed to accept requests for the best-known value.
     */
    private void onPromise(String sourceNodeId, PaxosPayload payload) {
        // If we are not the leader or we've already completed the prepare phase, ignore
        if (!isLeader || preparePhaseCompleted) {
            return;
        }
        int proposalNumber = payload.getProposalNumber();
        // If the incoming PROMISE is for a stale proposal, ignore
        if (proposalNumber != currentProposalNumber) {
            return;
        }

        // Bump our promiseCount for the current proposal
        proposerState.incrementPromiseCount();
        // Update our knowledge of the highest accepted proposal from the cluster
        proposerState.updateHighestAccepted(payload.getAcceptedId(), payload.getAcceptedValue());

        appLogger.info("Received PROMISE from {} for proposal #{} (count = {})",
                sourceNodeId, proposalNumber, proposerState.getPromiseCount());

        // If we reach our required quorum, we finalize the prepare phase
        if (proposerState.getPromiseCount() >= quorum) {
            // Cancel the pending prepare-timeout task
            if (prepareTimeoutTask != null) {
                prepareTimeoutTask.cancel(false);
                prepareTimeoutTask = null;
            }
            // If any node had a previously accepted value for a higher accepted ID, adopt that
            Object valueToAccept = (proposerState.getHighestAcceptedValue() != null)
                    ? proposerState.getHighestAcceptedValue()
                    : proposerState.getOriginalValue();

            preparePhaseCompleted = true;
            appLogger.info("Prepare phase complete with quorum reached. "
                    + "Moving to accept phase with value: {}", valueToAccept);

            // Broadcast the ACCEPT_REQUEST to all nodes
            broadcastAcceptRequest(currentProposalNumber, valueToAccept);
        }
    }

    /**
     * Handles an incoming ACCEPT_REQUEST from the leader.
     * If the proposalNumber >= promisedId, we accept the proposal
     * and respond with ACCEPTED.
     */
    private void onAcceptRequest(String sourceNodeId, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();
        if (proposalNumber >= promisedId) {
            // Accept
            promisedId = proposalNumber;
            acceptedId = proposalNumber;
            acceptedValue = payload.getProposedValue();

            // Send ACCEPTED back to the leader
            PaxosPayload response = new PaxosPayload();
            response.setProposalNumber(proposalNumber);
            response.setProposedValue(acceptedValue);

            SimulationMessage msg = SimulationMessageFactory.createMessage(
                    localNodeId,
                    sourceNodeId,
                    MessageType.ACCEPTED,
                    response,
                    ProtocolType.MULTI_PAXOS
            );
            router.messageSent(msg);

            appLogger.info("Node {} accepted proposal #{} from {}", localNodeId, proposalNumber, sourceNodeId);

        } else {
            // Rejected
            appLogger.info("Node {} rejected ACCEPT_REQUEST for proposal #{} (promisedId = {})",
                    localNodeId, proposalNumber, promisedId);
        }
    }

    /**
     * Handles ACCEPTED messages on the leader side. Once we have a quorum of ACCEPTED,
     * we commit the value.
     */
    private void onAccepted(String sourceNodeId, PaxosPayload payload) {
        if (!isLeader) {
            return;
        }
        int proposalNumber = payload.getProposalNumber();
        // If this ACCEPTED pertains to an older proposal, ignore
        if (proposalNumber != currentProposalNumber) {
            return;
        }

        // Tally this ACCEPTED response
        acceptResponseCount++;
        appLogger.info("Received ACCEPTED from {} for proposal #{} (count = {})",
                sourceNodeId, proposalNumber, acceptResponseCount);

        // If we’ve reached quorum on ACCEPTED responses, commit the value
        if (acceptResponseCount >= quorum) {
            Object valueToCommit = payload.getProposedValue();
            appLogger.info("Quorum reached on ACCEPTED responses for proposal #{}. Committing value: {}",
                    proposalNumber, valueToCommit);
            commit(valueToCommit);
        }
    }

    /**
     * Handles a COMMIT message from the leader (or another node).
     * We update our local {@link #committedValue} with the committed proposal.
     */
    private void onCommit(String sourceNodeId, PaxosPayload payload) {
        committedValue = payload.getProposedValue();
        appLogger.info("Node {} received COMMIT from {} for proposal #{} with value: {}",
                localNodeId, sourceNodeId, payload.getProposalNumber(), committedValue);
    }
}