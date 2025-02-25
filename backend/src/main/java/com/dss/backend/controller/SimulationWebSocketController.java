package com.dss.backend.controller;

import com.dss.backend.dto.EventDTO;
import com.dss.backend.metrics.MetricsSnapshot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

@Controller
public class SimulationWebSocketController {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    /**
     * Pushes a metrics update to the clients subscribed to a particular simulation.
     *
     * @param simulationId The simulation identifier.
     * @param snapshot     The current metrics snapshot.
     */
    public void sendMetricsUpdate(String simulationId, MetricsSnapshot snapshot) {
        messagingTemplate.convertAndSend("/topic/simulation/" + simulationId + "/metrics", snapshot);
    }

    /**
     * Pushes an event update to the clients subscribed to a particular simulation.
     *
     * @param simulationId The simulation identifier.
     * @param event        The event data to push.
     */
    public void sendEventUpdate(String simulationId, EventDTO event) {
        messagingTemplate.convertAndSend("/topic/simulation/" + simulationId + "/events", event);
    }
}