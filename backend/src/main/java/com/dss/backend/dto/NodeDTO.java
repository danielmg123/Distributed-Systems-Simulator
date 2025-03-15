package com.dss.backend.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
@Schema(description = "Data Transfer Object representing a node in the simulation.")
public class NodeDTO {

    @Schema(description = "Unique identifier of the node", example = "node1")
    private String id;

    @Schema(description = "Network address of the node", example = "192.168.1.1")
    private String address;

    @Schema(description = "Current status of the node", example = "ACTIVE")
    private String status;
}