package com.dss.backend.consensus.paxos;

import com.dss.backend.consensus.util.ConsensusBroadcaster;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.ProtocolType;

/**
 * <strong>PaxosMessagingUtil</strong> is a convenience class that packages common
 * broadcasting operations used during the Paxos protocol phases. 
 * <p>
 * Instead of duplicating logic in multiple places, methods like
 * {@code broadcastPrepareRequest} and {@code broadcastAcceptRequest}
 * provide a clean interface for sending Paxos messages (e.g., PREPARE_REQUEST, ACCEPT_REQUEST, COMMIT).
 * </p>
 * <p>
 * <strong>Key Design:</strong>
 * <ul>
 *   <li>This class centralizes Paxos-specific message creation, reducing repetition 
 *       in the core {@code PaxosAlgorithm} code.</li>
 *   <li>All methods require a {@link ConsensusBroadcaster} to handle the actual broadcast 
 *       mechanics (i.e. routing to all other nodes), preserving testability and
 *       separation of concerns.</li>
 * </ul>
 * <p>
 *
 * @author Daniel Morales
 */
public class PaxosMessagingUtil {

    /**
     * Broadcasts a PREPARE_REQUEST message to all nodes. 
     * <p>This corresponds to <em>Phase 1</em> of Paxos, where the Proposer 
     * asks Acceptors to promise not to accept proposals below the given proposal number.</p>
     *
     * @param broadcaster     utility for sending messages to all other nodes
     * @param proposalNumber  the proposal number for this Paxos instance
     * @param originalValue   the value the Proposer wants to eventually propose
     */
    public static void broadcastPrepareRequest(ConsensusBroadcaster broadcaster, int proposalNumber, Object originalValue) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(originalValue);
        broadcaster.broadcast(MessageType.PREPARE_REQUEST, payload, ProtocolType.PAXOS);
    }

    /**
     * Broadcasts an ACCEPT_REQUEST message (Paxos Phase 2).
     * <p>Once the Proposer has enough PROMISEs, it sends ACCEPT_REQUEST 
     * to attempt to get Acceptors to accept its proposal number and chosen value.</p>
     *
     * @param broadcaster     utility for sending messages to all nodes
     * @param proposalNumber  the proposal number being accepted
     * @param value           the value to propose in Phase 2
     */
    public static void broadcastAcceptRequest(ConsensusBroadcaster broadcaster, int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);
        broadcaster.broadcast(MessageType.ACCEPT_REQUEST, payload, ProtocolType.PAXOS);
    }

    /**
     * Broadcasts a COMMIT message to all nodes indicating that
     * a particular proposal has been chosen (accepted by a quorum).
     * <p>This is an optional step in basic Paxos (some variants 
     * rely on Learner logic), but many implementations 
     * broadcast a final COMMIT to ensure all nodes learn the chosen value.</p>
     *
     * @param broadcaster     utility for sending messages
     * @param proposalNumber  the winning/accepted proposal number
     * @param value           the chosen value
     */
    public static void broadcastCommit(ConsensusBroadcaster broadcaster, int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);
        broadcaster.broadcast(MessageType.COMMIT, payload, ProtocolType.PAXOS);
    }
}