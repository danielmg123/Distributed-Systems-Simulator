package com.dss.backend.mapper;

import com.dss.backend.dto.SimulationDTO;
import com.dss.backend.model.Simulation;
import org.mapstruct.Mapper;

/**
 * {@code SimulationMapper} maps between the domain {@link Simulation}
 * and the transfer-friendly {@link SimulationDTO}.
 *
 * <p>This mapping commonly includes nested fields such as
 * the simulation's {@code events} list or {@code config}.
 *
 * <p><strong>Usage:</strong></p>
 * <ul>
 *   <li>Exposing simulation details via a REST endpoint in a structured {@link SimulationDTO} format.</li>
 *   <li>Accepting a {@link SimulationDTO} as input and converting it to a {@link Simulation}
 *       for creation or update in MongoDB.</li>
 * </ul>
 */
@Mapper(componentModel = "spring")
public interface SimulationMapper {

    /**
     * Converts a domain {@link Simulation} to its {@link SimulationDTO}.
     *
     * @param simulation the Simulation object from the data layer
     * @return the DTO used by controllers
     */
    SimulationDTO simulationToSimulationDTO(Simulation simulation);

    /**
     * Converts a {@link SimulationDTO} back into a domain {@link Simulation}.
     *
     * @param simulationDTO the incoming Simulation DTO
     * @return the fully populated Simulation entity
     */
    Simulation simulationDTOToSimulation(SimulationDTO simulationDTO);
}