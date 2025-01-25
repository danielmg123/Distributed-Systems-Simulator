package com.dss.backend.dto;

import lombok.Data;
import java.time.LocalDateTime;
import com.dss.backend.model.EventType;

@Data
public class EventDTO {
    private EventType type;
    private String details;
    private LocalDateTime timestamp;
}