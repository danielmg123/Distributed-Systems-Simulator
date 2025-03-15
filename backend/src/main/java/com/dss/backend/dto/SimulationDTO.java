package com.dss.backend.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import java.util.List;
import com.dss.backend.model.SimulationStatus;

@Data
@Schema(description = "Data Transfer Object representing a simulation instance.")
public class SimulationDTO {

    @Schema(description = "Unique identifier of the simulation", example = "sim1")
    private String id;

    @Schema(description = "Name of the simulation", example = "Test Simulation")
    private String name;

    @Schema(description = "List of events that occurred during the simulation")
    private List<EventDTO> events;

    @Schema(description = "Current status of the simulation", example = "RUNNING")
    private SimulationStatus status;

    @Schema(description = "Configuration settings for the simulation")
    private SimulationConfigDTO config;
}