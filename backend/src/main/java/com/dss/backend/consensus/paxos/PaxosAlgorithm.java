package com.dss.backend.consensus.paxos;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.dss.backend.consensus.AbstractConsensusAlgorithm;
import com.dss.backend.consensus.ConsensusObserver;
import com.dss.backend.consensus.util.ConsensusBroadcaster;
import com.dss.backend.consensus.util.ConsensusUtils;
import com.dss.backend.messaging.*;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;

/**
 * Implements the basic Paxos consensus protocol.
 *
 * <p><strong>Approach:</strong></p>
 * <ul>
 *   <li>Each node can act as a <em>Proposer</em>, <em>Acceptor</em>, or <em>Learner</em>.</li>
 *   <li>The protocol operates in two phases:
 *     <ul>
 *       <li><em>Prepare/Promise</em> (Phase 1): A Proposer asks Acceptors to promise not to
 *       accept proposals below a given proposal number, ensuring the Proposer obtains
 *       the highest accepted proposal.</li>
 *       <li><em>Accept/Accepted</em> (Phase 2): Once the Proposer knows the highest
 *       accepted value (if any), it broadcasts an accept request with its proposal number
 *       and chosen value; Acceptors respond “ACCEPTED” if they have not promised a higher
 *       proposal number.</li>
 *     </ul>
 *   </li>
 *   <li>A majority (quorum) of Acceptors must accept a proposal for it to become chosen.</li>
 * </ul>
 *
 * <p><strong>Key Design Decisions:</strong></p>
 * <ul>
 *   <li>We generate proposal numbers by incrementing an atomic counter. In a more advanced
 *       deployment, we might incorporate unique node IDs or timestamps to ensure global uniqueness.</li>
 *   <li>Majority threshold is {@code (allNodeIds.size() / 2) + 1}; no dynamic membership changes or reconfiguration
 *       are handled in this implementation.</li>
 *   <li>Message types are strongly typed (e.g., {@code PREPARE_REQUEST}, {@code ACCEPT_REQUEST}, etc.)
 *       with distinct payload objects for clarity.</li>
 * </ul>
 *
 * <p><strong>Known Limitations:</strong></p>
 * <ul>
 *   <li>This basic Paxos does not include leader election or Multi-Paxos optimizations (which reduce
 *       repeated Phase 1 overhead once a stable leader is established).</li>
 *   <li>No advanced fault handling (e.g., if the Proposer fails mid-protocol, a new Proposer
 *       must start a higher proposal number and re-run Phase 1).</li>
 *   <li>There is no concurrency control if multiple nodes simultaneously become Proposers.
 *       This protocol is safe but may degrade performance in practice.</li>
 * </ul>
 *
 * <p>By extending {@link AbstractConsensusAlgorithm}, we inherit no-op implementations for methods
 * that Paxos does not use (e.g., certain commit methods). The primary logic is in
 * {@link #propose(Object)}, {@link #handleMessage(SimulationMessage)}, and the private methods
 * supporting the <em>prepare</em> and <em>accept</em> phases.</p>
 *
 * @author Daniel Morales
 */
public class PaxosAlgorithm extends AbstractConsensusAlgorithm {

    /** Logger for internal debug and info statements. */
    private final AppLogger appLogger = new DefaultAppLogger(PaxosAlgorithm.class);

    /** Per-node Paxos state: records the highest promise, accepted value, etc. */
    private final PaxosState paxosState;

    /** Message router for sending and receiving SimulationMessages between nodes. */
    private final MessageRouter router;

    /** All node IDs in the cluster (including this node). */
    private final List<String> allNodeIds;

    /** The local node's ID, used for logging and routing decisions. */
    private final String myNodeId;

    /** Helper object to broadcast messages to all nodes except self. */
    private final ConsensusBroadcaster broadcaster;

    /** Notified when this proposer chooses a value; defaults to no-op. */
    private volatile ConsensusObserver observer = ConsensusObserver.NO_OP;

    /** An atomic counter to generate unique proposal numbers. */
    private final AtomicInteger proposalCounter = new AtomicInteger(0);

    /**
     * The majority threshold needed for quorum. In a cluster of size N,
     * majority is (N/2 + 1).
     */
    private final int majority;

    /**
     * Tracks the ProposerState for each proposal number, so the local node
     * can manage multiple proposals concurrently if needed (though typically
     * they'd be handled sequentially).
     */
    private final ConcurrentHashMap<Integer, ProposerState> proposalStateMap = new ConcurrentHashMap<>();

    /**
     * Tracks how many ACCEPTED responses we've received for each proposal number
     * in Phase 2, so we know when we reach quorum.
     */
    private final ConcurrentHashMap<Integer, Integer> acceptCountMap = new ConcurrentHashMap<>();

    /**
     * This node's position within {@link #allNodeIds}. Salted into every proposal
     * number so that concurrently proposing nodes can never generate colliding
     * proposal numbers (see {@link #generateNextProposalNumber()}).
     */
    private final int nodeIndex;

    /**
     * Constructs a PaxosAlgorithm for this node.
     *
     * @param myNodeId   the local node ID
     * @param allNodeIds list of all participating node IDs (including this node)
     * @param router     the shared message router for sending and receiving messages
     */
    public PaxosAlgorithm(String myNodeId, List<String> allNodeIds, MessageRouter router) {
        this.myNodeId = myNodeId;
        this.allNodeIds = allNodeIds;
        this.router = router;
        this.paxosState = new PaxosState(myNodeId);
        this.broadcaster = new ConsensusBroadcaster(router, myNodeId);
        // For N nodes, the majority threshold is (N/2 + 1).
        this.majority = (allNodeIds.size() / 2) + 1;
        this.nodeIndex = allNodeIds.indexOf(myNodeId);
    }

    /**
     * Initiates Paxos by sending a PREPARE_REQUEST to each node
     * (i.e., beginning Phase 1: "Prepare").
     *
     * @param value the value/command we want to propose
     */
    @Override
    public void propose(Object value) {
        // Generate a new proposal number: must be strictly larger
        // than any we have used so far.
        int proposalNumber = generateNextProposalNumber();

        // Create a new ProposerState to track how many PROMISEs we've received, etc.
        proposalStateMap.put(proposalNumber, new ProposerState(value));

        // Broadcast the PREPARE_REQUEST (Phase 1) to all nodes.
        broadcastPrepareRequest(proposalNumber, value);
    }

    /**
     * Accept is not called directly by the Proposer in standard Paxos.
     * Instead, "accept" decisions are triggered by receiving an ACCEPT_REQUEST
     * message (see {@link #onAcceptRequest(String, PaxosPayload)}).
     *
     * @param proposal ignored in this implementation
     * @return always false
     */
    @Override
    public boolean accept(Object proposal) {
        // Not directly used in Paxos – acceptance is handled via message processing.
        return false;
    }

    /**
     * Marks the chosen value in our local PaxosState and logs it.
     * Typically, once a node learns a proposal is chosen, it can apply that
     * value to its local state machine.
     *
     * @param value the chosen value
     */
    @Override
    public void commit(Object value) {
        // Mark the chosen value in the PaxosState so this node
        // knows it has been committed.
        paxosState.setChosenValue(value);
        appLogger.info("Node {} has COMMITTED value: {}", myNodeId, value);
    }

    /**
     * @return the value this node has learned as chosen by the cluster, or {@code null}
     *         if it hasn't learned one yet. Exposed for tests and inspection.
     */
    public Object getChosenValue() {
        return paxosState.getChosenValue();
    }

    @Override
    public void setConsensusObserver(ConsensusObserver observer) {
        this.observer = (observer != null) ? observer : ConsensusObserver.NO_OP;
    }

    /**
     * Main entry point for handling messages from other nodes.
     * Based on the message type, we dispatch to the appropriate
     * Phase 1 (prepare/promise) or Phase 2 (accept/accepted) handler.
     *
     * @param msg the incoming message
     */
    @Override
    public void handleMessage(SimulationMessage msg) {
        // Safely cast the payload to PaxosPayload (or skip if invalid).
        PaxosPayload payload = ConsensusUtils.safeCastPayload(msg, PaxosPayload.class);
        if (payload == null) {
            return;
        }
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
            case COMMIT:
                onCommit(msg.getSourceNodeId(), payload);
                break;
            default:
                // For Paxos, we ignore any other message types (heartbeat, etc.).
                break;
        }
    }

    // ---------------------- Phase 1: Prepare / Promise ----------------------

    /**
     * Broadcasts a PREPARE_REQUEST message (start of Phase 1) to all nodes.
     *
     * @param proposalNumber unique proposal number for this attempt
     * @param originalValue  the value we eventually want to propose
     */
    private void broadcastPrepareRequest(int proposalNumber, Object originalValue) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(originalValue);

        // Send to everyone except self.
        broadcaster.broadcast(MessageType.PREPARE_REQUEST, payload, ProtocolType.PAXOS);
    }

    /**
     * Handles an incoming PREPARE_REQUEST from a Proposer.
     * <p>
     * If the incoming proposal number is >= the highest we have promised,
     * we "promise" not to accept proposals with lower numbers.
     * We then respond with a PROMISE message containing our current accepted proposal
     * (if any), so the Proposer can pick the highest accepted value.
     *
     * @param sourceNode the node that sent the request
     * @param payload    the PaxosPayload with the proposal number and potential value
     */
    private void onPrepareRequest(String sourceNode, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();

        // If the incoming proposalNumber is >= promisedId, we
        // promise not to accept proposals with lower IDs.
        if (proposalNumber >= paxosState.getPromisedId()) {
            paxosState.setPromisedId(proposalNumber);

            // We respond with a PROMISE that includes the highest accepted proposal
            // we already have (acceptedId and acceptedValue), if any.
            PaxosPayload reply = new PaxosPayload();
            reply.setProposalNumber(proposalNumber);
            reply.setAcceptedId(paxosState.getAcceptedId());
            reply.setAcceptedValue(paxosState.getAcceptedValue());

            // Send PROMISE back to the Proposer.
            SimulationMessage promiseMsg = SimulationMessageFactory.createMessage(
                    myNodeId,
                    sourceNode,
                    MessageType.PROMISE,
                    reply,
                    ProtocolType.PAXOS);
            router.messageSent(promiseMsg);
        }
        // If the proposalNumber < promisedId, we do nothing
        // (implicitly "reject"), or we could explicitly send a rejection.
    }

    /**
     * Processes a PROMISE message from an Acceptor (Phase 1).
     * <p>
     * We track how many PROMISE messages we've received for the specified proposal number.
     * Once we hit the majority threshold, we proceed to Phase 2 by broadcasting
     * an ACCEPT_REQUEST with the "best" accepted value from all PROMISES.
     *
     * @param sourceNodeId the node that sent the PROMISE
     * @param payload       includes the proposalNumber, acceptedId, and acceptedValue
     */
    private void onPromise(String sourceNodeId, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();

        // Retrieve the ProposerState for this proposal. If null, we might have
        // moved on or it's a stale PROMISE, so we ignore it.
        ProposerState state = proposalStateMap.get(proposalNumber);
        if (state == null) {
            // Ignore stale or duplicate PROMISE messages.
            return;
        }

        // Bump the count of how many PROMISEs we have for this proposal number.
        state.incrementPromiseCount();
        // If the Acceptor had previously accepted a higher proposal, record it
        // in highestAcceptedId/highestAcceptedValue.
        state.updateHighestAccepted(payload.getAcceptedId(), payload.getAcceptedValue());

        appLogger.info("Received PROMISE from {} for proposal #{} (count = {})",
                sourceNodeId, proposalNumber, state.getPromiseCount());

        // Once we reach the majority, we move to Accept phase.
        if (state.getPromiseCount() >= majority) {
            // Choose whichever value has the highest acceptedId from the PROMISEs,
            // or if none, use the original value we wanted to propose.
            Object valueToAccept = (state.getHighestAcceptedValue() != null)
                    ? state.getHighestAcceptedValue()
                    : state.getOriginalValue();

            // Broadcast ACCEPT_REQUEST for Phase 2.
            broadcastAcceptRequest(proposalNumber, valueToAccept);
        }
    }

    // ---------------------- Phase 2: Accept / Accepted ----------------------

    /**
     * Broadcasts an ACCEPT_REQUEST message (Phase 2) to all nodes.
     * This requests that Acceptors accept our proposal number and chosen value.
     *
     * @param proposalNumber the proposal number we're using
     * @param value          the value to accept
     */
    private void broadcastAcceptRequest(int proposalNumber, Object value) {
        // Initialize the acceptCount for this proposal to 0.
        acceptCountMap.put(proposalNumber, 0);

        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);

        // Send ACCEPT_REQUEST to every node, including ourselves
        // (though typically we might skip self if we already accept implicitly).
        for (String nodeId : allNodeIds) {
            SimulationMessage acceptRequestMsg = SimulationMessageFactory.createMessage(
                    myNodeId,
                    nodeId,
                    MessageType.ACCEPT_REQUEST,
                    payload,
                    ProtocolType.PAXOS
            );
            router.messageSent(acceptRequestMsg);
        }
    }

    /**
     * Handles an incoming ACCEPT_REQUEST.
     * <p>
     * If the proposalNumber is >= the highest we have promised, we accept
     * by updating our PaxosState (acceptedId, acceptedValue) and sending back ACCEPTED.
     * Otherwise, we reject (do nothing or log a message).
     *
     * @param sourceNode the node that sent the ACCEPT_REQUEST
     * @param payload    includes the proposalNumber and the proposedValue
     */
    private void onAcceptRequest(String sourceNode, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();

        // If the proposalNumber is >= our promisedId, accept it.
        if (proposalNumber >= paxosState.getPromisedId()) {
            paxosState.setAcceptedId(proposalNumber);
            paxosState.setAcceptedValue(payload.getProposedValue());

            // Construct an ACCEPTED message to confirm our acceptance to the proposer.
            PaxosPayload acceptedPayload = new PaxosPayload();
            acceptedPayload.setProposalNumber(proposalNumber);
            acceptedPayload.setProposedValue(payload.getProposedValue());

            SimulationMessage acceptedMsg = SimulationMessageFactory.createMessage(
                    myNodeId,
                    sourceNode,
                    MessageType.ACCEPTED,
                    acceptedPayload,
                    ProtocolType.PAXOS
            );
            router.messageSent(acceptedMsg);

            appLogger.info("Accepted proposal #{} from {}", proposalNumber, sourceNode);
        } else {
            // If it's below our promisedId, we effectively "reject".
            appLogger.info("Rejected ACCEPT_REQUEST for proposal #{} (promisedId = {})",
                    proposalNumber, paxosState.getPromisedId());
        }
    }

    /**
     * Processes an ACCEPTED response for our proposal.
     * <p>
     * Once a quorum of ACCEPTED responses is reached, we commit the value
     * and mark it as chosen in our PaxosState.
     *
     * @param sourceNode the node that sent the ACCEPTED response
     * @param payload    includes the proposalNumber and the proposedValue that was accepted
     */
    private void onAccepted(String sourceNode, PaxosPayload payload) {
        int proposalNumber = payload.getProposalNumber();

        // Get the current count of ACCEPTED responses for this proposal.
        Integer oldCount = acceptCountMap.get(proposalNumber);
        if (oldCount == null) {
            // Possibly a stale or irrelevant ACCEPTED message.
            return;
        }

        // Increment the count and update the map.
        int newCount = oldCount + 1;
        acceptCountMap.put(proposalNumber, newCount);

        // If we've reached the majority threshold, commit the value.
        if (newCount >= majority) {
            commit(payload.getProposedValue());
            observer.onCommitted(myNodeId, payload.getProposedValue());

            // Without this, only the proposer ever learns the chosen value -- the
            // acceptors that voted ACCEPTED never find out their proposal was chosen.
            // Broadcasting COMMIT here is the Learner phase: it disseminates the
            // chosen value to every node, not just the proposer.
            PaxosMessagingUtil.broadcastCommit(broadcaster, proposalNumber, payload.getProposedValue());

            // Optionally, we could remove references to that proposal from
            // proposalStateMap and acceptCountMap for cleanup.
        }
    }

    /**
     * Handles an incoming COMMIT message (the Learner phase). Any node -- proposer
     * or acceptor -- that receives this learns the value chosen by the cluster and
     * applies it locally via {@link #commit(Object)}.
     *
     * @param sourceNode the node that sent the COMMIT (the original proposer)
     * @param payload    includes the proposalNumber and the chosen value
     */
    private void onCommit(String sourceNode, PaxosPayload payload) {
        commit(payload.getProposedValue());
    }

    // ---------- Helper: Generate Unique Proposal IDs ----------

    /**
     * Generates a globally unique proposal number by combining a local,
     * strictly-increasing round counter with this node's index in
     * {@link #allNodeIds}. Plain per-node counters (e.g. 1, 2, 3, ...) would let
     * two different nodes generate the same proposal number concurrently, which
     * breaks Paxos's safety guarantees since acceptors compare proposal numbers
     * as plain integers across all proposers. Salting by node index guarantees
     * every node produces a disjoint sequence of numbers.
     *
     * @return the next globally unique proposal number
     */
    private int generateNextProposalNumber() {
        int round = proposalCounter.incrementAndGet();
        return round * allNodeIds.size() + nodeIndex;
    }
}