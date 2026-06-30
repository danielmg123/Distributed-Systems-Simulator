package com.dss.backend.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import lombok.Data;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a simulation instance with a unique ID, a descriptive name,
 * and a collection of events that have happened during the simulation.
 *
 * <p><strong>Key Fields:</strong></p>
 * <ul>
 *   <li>{@code name} - Human-readable identifier for the simulation.</li>
 *   <li>{@code events} - A log of {@link Event}s that occurred during runtime.</li>
 *   <li>{@code status} - Tracks whether the simulation is RUNNING, PAUSED, COMPLETED, or ERROR.</li>
 *   <li>{@code config} - Holds configuration details in a {@link SimulationConfig}.</li>
 * </ul>
 *
 * <p><strong>Use Cases:</strong>
 * <ul>
 *   <li>Storing or retrieving the entire simulation in a DB.</li>
 *   <li>Displaying simulation progress or history in a UI timeline.</li>
 *   <li>Applying certain logic (like stopping or restarting) based on {@code status}.</li>
 * </ul>
 */
@Document
@Data
public class Simulation {

    @Id
    private String id;

    private String name;
    private List<Event> events = new ArrayList<>();
    private SimulationStatus status;

    private SimulationConfig config;
}