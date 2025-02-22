package com.dss.backend.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import lombok.Data;
import java.util.List;

@Document
@Data
public class Simulation {

    @Id
    private String id;

    private String name;
    private List<Event> events;
    private SimulationStatus status;

    private SimulationConfig config;
}
