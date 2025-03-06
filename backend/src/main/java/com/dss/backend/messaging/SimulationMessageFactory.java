package com.dss.backend.messaging;

public class SimulationMessageFactory {

    /**
     * Creates and returns a new SimulationMessage.
     *
     * @param sourceNodeId the sender’s node ID
     * @param targetNodeId the recipient’s node ID
     * @param type the message type
     * @param payload the message payload
     * @param protocol the protocol that is sending the message
     * @return a new SimulationMessage instance
     */
    public static SimulationMessage createMessage(String sourceNodeId, String targetNodeId,
                                                  MessageType type, Object payload,
                                                  ProtocolType protocol) {
        SimulationMessage msg = new SimulationMessage(sourceNodeId, targetNodeId, type, payload, protocol);
        msg.setProtocol(protocol);
        return msg;
    }
}