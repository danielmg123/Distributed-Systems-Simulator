package com.dss.backend.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import lombok.Data;

@Document
@Data
public class Node {

    @Id
    private String id;
    private String address;
    private NodeStatus status;
    // Additional properties...

    // Constructors, getters, and setters...
}