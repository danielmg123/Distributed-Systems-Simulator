package com.dss.backend.logging;

/**
 * The {@code AppLogger} interface abstracts logging in the application,
 * allowing for different logging frameworks or custom implementations.
 *
 * <p>My implementations include {@link DefaultAppLogger},
 * but we can create others if different behavior are needed.</p>
 */
public interface AppLogger {

    /**
     * Logs a message at the INFO level.
     *
     * @param message The format string (with placeholders).
     * @param args    Optional arguments to fill in the placeholders.
     */
    void info(String message, Object... args);

    /**
     * Logs a message at the DEBUG level.
     *
     * @param message The format string (with placeholders).
     * @param args    Optional arguments to fill in the placeholders.
     */
    void debug(String message, Object... args);

    /**
     * Logs a message at the ERROR level.
     *
     * @param message The format string (with placeholders).
     * @param args    Optional arguments to fill in the placeholders.
     */
    void error(String message, Object... args);
}