package com.dss.backend.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Data;

import java.util.List;

@Document
@Data
public class NetworkTopology {

    @Id
    private String id;
    private List<Node> nodes;
    private TopologyType type;
    // Additional properties...

    // Constructors, getters, and setters...
}
