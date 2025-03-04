package com.dss.backend.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultAppLogger implements AppLogger {
    private final Logger logger;

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