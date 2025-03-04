package com.dss.backend.service.engine;

import com.dss.backend.dto.EventDTO;
import com.dss.backend.model.Event;
import com.dss.backend.controller.SimulationWebSocketController;
import com.dss.backend.model.EventType;

import java.time.LocalDateTime;

public class EventLoggerService {

    private final SimulationWebSocketController webSocketController;

    public EventLoggerService(SimulationWebSocketController webSocketController) {
        this.webSocketController = webSocketController;
    }

    /**
     * Logs an event by creating an EventDTO and sending it via the WebSocket controller.
     *
     * @param simulationId the simulation identifier
     * @param message      the event message
     * @param eventType    the type of event
     */
    public void logEvent(String simulationId, String message, EventType eventType) {
        Event event = new Event();
        event.setType(eventType);
        event.setDetails(message);
        event.setTimestamp(LocalDateTime.now());

        EventDTO dto = mapEventToDTO(event);
        webSocketController.sendEventUpdate(simulationId, dto);
        // (Optionally persist the event in a database.)
    }

    public EventDTO mapEventToDTO(Event event) {
        EventDTO dto = new EventDTO();
        dto.setType(event.getType());
        dto.setDetails(event.getDetails());
        dto.setTimestamp(event.getTimestamp());
        return dto;
    }
}