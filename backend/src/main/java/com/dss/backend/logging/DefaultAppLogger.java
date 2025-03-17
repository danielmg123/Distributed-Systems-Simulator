package com.dss.backend.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code DefaultAppLogger} is the standard implementation of {@link AppLogger},
 * delegating calls to an underlying SLF4J logger.
 *
 * <p>This class provides:</p>
 * <ul>
 *   <li>Automatic logging framework resolution via SLF4J.</li>
 *   <li>Placeholder-based logging (e.g. {@code logger.info("Message: {}", value)}).</li>
 *   <li>Consistent usage across the application, so you can switch implementations if needed.</li>
 * </ul>
 */
public class DefaultAppLogger implements AppLogger {

    private final Logger logger;

    /**
     * Constructs a {@code DefaultAppLogger} for the given class.
     *
     * @param clazz the class for which logs will be produced
     */
    public DefaultAppLogger(Class<?> clazz) {
        this.logger = LoggerFactory.getLogger(clazz);
    }

    @Override
    public void info(String message, Object... args) {
        logger.info(message, args);
    }

    @Override
    public void debug(String message, Object... args) {
        logger.debug(message, args);
    }

    @Override
    public void error(String message, Object... args) {
        logger.error(message, args);
    }
}