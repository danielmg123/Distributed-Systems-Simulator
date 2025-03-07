package com.dss.backend.consensus.util;

import com.dss.backend.messaging.SimulationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConsensusUtils {
    private static final Logger logger = LoggerFactory.getLogger(ConsensusUtils.class);

    // prevent instantiation
    private ConsensusUtils() {}

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