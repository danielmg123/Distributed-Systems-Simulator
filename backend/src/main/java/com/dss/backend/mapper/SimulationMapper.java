package com.dss.backend.mapper;

import com.dss.backend.dto.SimulationDTO;
import com.dss.backend.model.Simulation;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface SimulationMapper {

    SimulationDTO simulationToSimulationDTO(Simulation simulation);

    Simulation simulationDTOToSimulation(SimulationDTO simulationDTO);
}
