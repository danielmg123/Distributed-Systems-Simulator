package com.dss.backend.model;

import java.time.LocalDateTime;
import lombok.Data;

/**
 * Represents a discrete event within the simulation, such as a node failure
 * or a message being sent.
 *
 * <p><strong>Notes:</strong></p>
 * <ul>
 *   <li>This class is often used to capture timeline data for UI displays or logs.</li>
 *   <li>Events can be persisted as part of the {@link Simulation} or just logged.</li>
 * </ul>
 */
@Data
public class Event {

    /**
     * The high-level category of the event (e.g. NODE_FAILED, SIMULATION_EVENT).
     */
    private EventType type;

    /**
     * Describes the event details (e.g. "Node X crashed unexpectedly").
     */
    private String details;

    /**
     * Timestamp (local date-time) indicating when this event occurred.
     */
    private LocalDateTime timestamp;
}