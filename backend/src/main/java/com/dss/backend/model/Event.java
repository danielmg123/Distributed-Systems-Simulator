package com.dss.backend.model;
import java.time.LocalDateTime;
import lombok.Data;

@Data
public class Event {
    private EventType type;
    private String details;
    private LocalDateTime timestamp;
    // Additional properties...
}

