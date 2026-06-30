package com.dss.backend.exception;

/**
 * Thrown when a request conflicts with a simulation's current run state -- e.g. trying
 * to run a simulation that is already running, or acting on one (proposing, failing or
 * recovering a node, tuning network conditions, reading live node statuses or topology)
 * that is not currently running. Maps to HTTP 409 Conflict so the caller can tell the
 * difference between "this simulation does not exist" (404) and "this simulation is not
 * in a state that can serve the request" (409).
 */
public class SimulationConflictException extends RuntimeException {

    public SimulationConflictException(String message) {
        super(message);
    }
}
