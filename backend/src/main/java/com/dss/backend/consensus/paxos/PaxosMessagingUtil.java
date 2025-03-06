// File: backend/src/main/java/com/dss/backend/consensus/paxos/PaxosMessagingUtil.java
package com.dss.backend.consensus.paxos;

import com.dss.backend.consensus.util.ConsensusBroadcaster;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.ProtocolType;
import com.dss.backend.messaging.SimulationMessageFactory;

public class PaxosMessagingUtil {

    public static void broadcastPrepareRequest(ConsensusBroadcaster broadcaster, int proposalNumber, Object originalValue) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(originalValue);
        broadcaster.broadcast(MessageType.PREPARE_REQUEST, payload, ProtocolType.PAXOS);
    }

    public static void broadcastAcceptRequest(ConsensusBroadcaster broadcaster, int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);
        broadcaster.broadcast(MessageType.ACCEPT_REQUEST, payload, ProtocolType.PAXOS);
    }

    public static void broadcastCommit(ConsensusBroadcaster broadcaster, int proposalNumber, Object value) {
        PaxosPayload payload = new PaxosPayload();
        payload.setProposalNumber(proposalNumber);
        payload.setProposedValue(value);
        broadcaster.broadcast(MessageType.COMMIT, payload, ProtocolType.PAXOS);
    }
}