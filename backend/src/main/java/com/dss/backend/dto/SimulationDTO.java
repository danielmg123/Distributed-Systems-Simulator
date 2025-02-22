package com.dss.backend.dto;

import com.dss.backend.model.SimulationStatus;
import lombok.Data;
import java.util.List;

@Data
public class SimulationDTO {
    private String id;
    private String name;
    private List<EventDTO> events;
    private SimulationStatus status;
    private SimulationConfigDTO config;
}