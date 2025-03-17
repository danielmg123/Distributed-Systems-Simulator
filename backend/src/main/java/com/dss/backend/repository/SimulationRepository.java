package com.dss.backend.repository;

import com.dss.backend.model.Simulation;
import com.dss.backend.model.SimulationStatus;
import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * {@code SimulationRepository} manages persistence of {@link Simulation} documents.
 *
 * <p>In addition to the core CRUD methods from {@link MongoRepository},
 * it provides custom finder methods:</p>
 * <ul>
 *   <li>{@link #findByStatus(SimulationStatus)} – Lists all simulations currently
 *       at a specified status (RUNNING, PAUSED, COMPLETED, etc.).</li>
 *   <li>{@link #findByName(String)} – Retrieves a simulation by its unique name.</li>
 * </ul>
 */
public interface SimulationRepository extends MongoRepository<Simulation, String> {

    /**
     * Finds all simulations with a specific status.
     *
     * @param status the simulation status to match (e.g. RUNNING, COMPLETED)
     * @return a list of simulations having that status
     */
    List<Simulation> findByStatus(SimulationStatus status);

    /**
     * Finds a single simulation by name.
     *
     * @param name the unique name of the simulation
     * @return the matching {@link Simulation}, or null if not found
     */
    Simulation findByName(String name);
}