package com.dss.backend.engine.service;

import com.dss.backend.engine.Scheduler;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.metrics.PerformanceMetricsCollector;
import com.dss.backend.controller.SimulationWebSocketController;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricsUpdateService {

    private final PerformanceMetricsCollector metricsCollector;
    private final SimulationWebSocketController webSocketController;
    private final Scheduler scheduler;

    public MetricsUpdateService(PerformanceMetricsCollector metricsCollector,
                                SimulationWebSocketController webSocketController,
                                Scheduler scheduler) {
        this.metricsCollector = metricsCollector;
        this.webSocketController = webSocketController;
        this.scheduler = scheduler;
    }

    /**
     * Schedules periodic metrics updates to be sent to subscribed WebSocket clients.
     *
     * @param simulationId the simulation identifier
     */
    public void startMetricsUpdates(String simulationId) {
        scheduler.scheduleAtFixedRate(() -> {
            MetricsSnapshot snapshot = metricsCollector.getSnapshot();
            webSocketController.sendMetricsUpdate(simulationId, snapshot);
        }, 0, 5, TimeUnit.SECONDS);
    }

    /**
     * Returns the current metrics snapshot.
     */
    public MetricsSnapshot getMetricsSnapshot() {
        return metricsCollector.getSnapshot();
    }
}