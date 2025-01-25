package com.dss.backend.dto;

import lombok.Data;
import java.util.List;

@Data
public class SimulationDTO {
    private String id;
    private String name;
    private List<EventDTO> events;
    private String status;
}