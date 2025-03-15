package com.dss.backend.consensus.paxos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <strong>PaxosPayload</strong> models the data carried in Paxos-specific messages.
 * <p>
 * It includes fields commonly used in Prepare and Accept phases:
 * <ul>
 *   <li>{@code proposalNumber}: identifies the proposal. Typically must be strictly increasing
 *       to ensure safety if multiple Proposers are active.</li>
 *   <li>{@code acceptedId} / {@code acceptedValue}: helps carry the highest accepted proposal
 *       info back to the Proposer (Phase 1/Promise).</li>
 *   <li>{@code proposedValue}: the value being proposed by the Proposer (Phase 2).</li>
 * </ul>
 * </p>
 * <p>
 * <strong>Usage:</strong>
 * <ul>
 *   <li>In <em>PREPARE_REQUEST</em> messages, only {@code proposalNumber} and
 *       {@code proposedValue} may be relevant.</li>
 *   <li>In <em>PROMISE</em>, {@code acceptedId} and {@code acceptedValue}
 *       let the Proposer learn if there's a previously accepted proposal
 *       it needs to adopt.</li>
 *   <li>For <em>ACCEPT_REQUEST</em> and <em>ACCEPTED</em>, the same payload can
 *       be reused with updated fields as necessary.</li>
 * </ul>
 *
 * @see com.dss.backend.consensus.paxos.PaxosAlgorithm
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaxosPayload {
    /** The proposal number for this Paxos instance (must be unique/monotonically increasing). */
    private int proposalNumber;

    /**
     * The ID (proposal number) of the highest proposal previously accepted
     * by the Acceptor. Used in Phase 1 responses to inform the Proposer.
     */
    private int acceptedId;

    /**
     * The value associated with {@link #acceptedId}, i.e.,
     * the actual data/command accepted under that proposal number.
     */
    private Object acceptedValue;

    /**
     * The new value that the Proposer wants acceptors to accept
     * (Phase 2). Also used in PREPARE requests to indicate the
     * original value the Proposer hopes to propose.
     */
    private Object proposedValue;
}