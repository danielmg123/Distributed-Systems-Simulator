package com.dss.backend.logging;

public interface AppLogger {
    void info(String message, Object... args);
    void debug(String message, Object... args);
    void error(String message, Object... args);
}