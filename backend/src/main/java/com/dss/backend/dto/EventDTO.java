package com.dss.backend.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import java.time.LocalDateTime;
import com.dss.backend.model.EventType;

@Data
@Schema(description = "Data Transfer Object for events generated during the simulation.")
public class EventDTO {

    @Schema(description = "Type of event", example = "SIMULATION_EVENT")
    private EventType type;

    @Schema(description = "Detailed description of the event", example = "Node failed due to timeout")
    private String details;

    @Schema(description = "Timestamp when the event occurred", example = "2025-03-14T15:30:00")
    private LocalDateTime timestamp;
}