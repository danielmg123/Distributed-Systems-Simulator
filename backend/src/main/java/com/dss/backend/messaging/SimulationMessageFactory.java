package com.dss.backend.messaging;

/**
 * A simple factory class for creating {@link SimulationMessage} instances.
 * <p>
 * This allows for a cleaner approach if for example, we wanted
 * to add metadata or transform the payload in a single place rather
 * than scattering <code>new SimulationMessage(...)</code> calls throughout the code.
 */
public class SimulationMessageFactory {

    /**
     * Creates and returns a new {@link SimulationMessage}. This is a convenience
     * method to ensure that all messages have consistent source, target, type, payload,
     * and protocol settings.
     *
     * @param sourceNodeId the ID of the node sending this message
     * @param targetNodeId the ID of the node receiving this message
     * @param type         the {@link MessageType} (e.g., PREPARE_REQUEST, COMMIT, HEARTBEAT, etc.)
     * @param payload      the data or object carried with the message
     * @param protocol     the consensus/communication {@link ProtocolType}
     * @return a fully populated {@link SimulationMessage} instance
     */
    public static SimulationMessage createMessage(String sourceNodeId,
                                                  String targetNodeId,
                                                  MessageType type,
                                                  Object payload,
                                                  ProtocolType protocol) {
        SimulationMessage msg = new SimulationMessage(sourceNodeId, targetNodeId, type, payload, protocol);
        msg.setProtocol(protocol);
        return msg;
    }
}