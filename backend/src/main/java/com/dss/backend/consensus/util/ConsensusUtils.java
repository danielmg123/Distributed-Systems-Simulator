package com.dss.backend.consensus.util;

import com.dss.backend.messaging.SimulationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collection of utility methods to support consensus algorithms.
 * <p>
 * Currently provides a safe cast of message payloads to reduce boilerplate checks
 * and simplify code in various message handlers.
 */
public final class ConsensusUtils {

    private static final Logger logger = LoggerFactory.getLogger(ConsensusUtils.class);

    /**
     * Private constructor prevents instantiation.
     */
    private ConsensusUtils() {
        // Utility class: no instances allowed.
    }

    /**
     * Safely casts the payload in a {@link SimulationMessage} to the specified type.
     * <p>
     * Logs an error if the message is null or if the payload is not an instance
     * of the requested class, and returns {@code null} in these cases.
     *
     * @param msg   The simulation message whose payload we want to cast.
     * @param clazz The {@link Class} to which we expect to cast the payload.
     * @param <T>   The type parameter corresponding to the desired payload type.
     * @return      The casted payload if it matches {@code clazz}, or {@code null} otherwise.
     */
    public static <T> T safeCastPayload(SimulationMessage msg, Class<T> clazz) {
        if (msg == null) {
            logger.error("Received null SimulationMessage");
            return null;
        }
        Object payload = msg.getPayload();
        if (clazz.isInstance(payload)) {
            return clazz.cast(payload);
        } else {
            logger.error("Payload type mismatch in message from {}: expected {}, but got {}",
                    msg.getSourceNodeId(),
                    clazz.getSimpleName(),
                    payload != null ? payload.getClass().getSimpleName() : "null");
            return null;
        }
    }
}