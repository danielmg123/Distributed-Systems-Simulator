package com.dss.backend.service.engine;

import com.dss.backend.dto.EventDTO;
import com.dss.backend.model.Event;
import com.dss.backend.controller.SimulationWebSocketController;
import com.dss.backend.model.EventType;

import java.time.LocalDateTime;

/**
 * Handles the creation and dispatching of simulation events (e.g. node failures,
 * start/stop signals, general logs) to relevant listeners. Uses the
 * {@link SimulationWebSocketController} to push event updates in real time
 * to subscribed clients.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Encapsulate the logic of constructing {@code Event} objects.</li>
 *   <li>Convert events to {@link EventDTO} and dispatch them via WebSocket.</li>
 *   <li>I can optionally, persist or record the event if desired (currently,
 *       it only sends them to the WebSocket layer).</li>
 * </ul>
 *
 * <p>This service is typically invoked by higher-level orchestrators, like
 * {@code SimulationOrchestrator}, to log significant simulation milestones
 * (e.g., a node failing or a simulation starting).
 */
public class EventLoggerService {

    private final SimulationWebSocketController webSocketController;

    /**
     * Creates an {@code EventLoggerService} that uses a
     * {@link SimulationWebSocketController} for broadcasting simulation events.
     *
     * @param webSocketController the controller responsible for dispatching
     *                            WebSocket messages to connected clients
     */
    public EventLoggerService(SimulationWebSocketController webSocketController) {
        this.webSocketController = webSocketController;
    }

    /**
     * Logs an event by creating an {@link Event} object, converting it to an
     * {@link EventDTO}, then sending it to clients via WebSocket.
     *
     * @param simulationId the simulation identifier
     * @param message      text describing what happened
     * @param eventType    the category/type of event (e.g., NODE_FAILED)
     */
    public void logEvent(String simulationId, String message, EventType eventType) {
        Event event = new Event();
        event.setType(eventType);
        event.setDetails(message);
        event.setTimestamp(LocalDateTime.now());

        EventDTO dto = mapEventToDTO(event);
        webSocketController.sendEventUpdate(simulationId, dto);
        // Additional persistence logic could be placed here if needed.
    }

    /**
     * Converts an {@link Event} domain object to the DTO used for
     * WebSocket broadcasting.
     *
     * @param event the domain event object
     * @return an {@link EventDTO} suitable for JSON/WS output
     */
    public EventDTO mapEventToDTO(Event event) {
        EventDTO dto = new EventDTO();
        dto.setType(event.getType());
        dto.setDetails(event.getDetails());
        dto.setTimestamp(event.getTimestamp());
        return dto;
    }
}