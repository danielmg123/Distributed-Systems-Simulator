package com.dss.backend.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import lombok.Data;

/**
 * Represents a node in the distributed system. Each node has:
 * <ul>
 *   <li>{@code id} - Unique identifier (also primary key in MongoDB).</li>
 *   <li>{@code address} - Simulated network address (e.g. IP or hostname).</li>
 *   <li>{@code status} - Indicates ACTIVE, INACTIVE, or FAILED state.</li>
 * </ul>
 *
 * <p><strong>Behavioral Notes:</strong></p>
 * <ul>
 *   <li>Nodes can fail or become inactive during the simulation; other nodes
 *       must handle that gracefully.</li>
 *   <li>Recovery, if implemented, can transition a FAILED node back to ACTIVE.</li>
 * </ul>
 */
@Document
@Data
public class Node {

    @Id
    private String id;
    private String address;
    private NodeStatus status;
}