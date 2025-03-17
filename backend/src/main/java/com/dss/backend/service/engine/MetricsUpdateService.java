package com.dss.backend.service.engine;

import com.dss.backend.engine.Scheduler;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.metrics.PerformanceMetricsCollector;
import com.dss.backend.controller.SimulationWebSocketController;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Manages periodic collection and broadcasting of simulation metrics.
 * <p>
 * Key Responsibilities:
 * <ul>
 *   <li>Retrieve metric data (e.g., total messages, latencies) from the
 *       {@link PerformanceMetricsCollector}.</li>
 *   <li>Schedule recurring tasks that push {@link MetricsSnapshot} objects
 *       to the front-end via a WebSocket topic.</li>
 *   <li>Integrate with the central {@link Scheduler} so that metrics updates
 *       occur at fixed intervals, avoiding ad hoc threading.</li>
 * </ul>
 *
 * <p>This service is typically instantiated by the {@code SimulationOrchestrator}
 * or a higher-level service that orchestrates the entire simulation lifecycle.
 */
public class MetricsUpdateService {

    private final PerformanceMetricsCollector metricsCollector;
    private final SimulationWebSocketController webSocketController;
    private final Scheduler scheduler;

    /**
     * Creates a {@link MetricsUpdateService} with references to the
     * collector, WebSocket controller, and scheduler.
     *
     * @param metricsCollector      collects and stores simulation metrics
     * @param webSocketController   used for pushing updates to clients
     * @param scheduler             schedules the periodic metrics tasks
     */
    public MetricsUpdateService(PerformanceMetricsCollector metricsCollector,
                                SimulationWebSocketController webSocketController,
                                Scheduler scheduler) {
        this.metricsCollector = metricsCollector;
        this.webSocketController = webSocketController;
        this.scheduler = scheduler;
    }

    /**
     * Schedules the recurring job that fetches a metrics snapshot from
     * the collector and publishes it to the connected clients.
     *
     * @param simulationId the unique identifier for the simulation whose
     *                     metrics are being monitored
     */
    public void startMetricsUpdates(String simulationId) {
        try {
            scheduler.scheduleAtFixedRate(() -> {
                        MetricsSnapshot snapshot = metricsCollector.getSnapshot();
                        webSocketController.sendMetricsUpdate(simulationId, snapshot);
                    },
                    0,          // initial delay
                    5,          // repeated period (5 seconds is a typical example)
                    TimeUnit.SECONDS);
        } catch (RejectedExecutionException ignored) {
            // If the scheduler was shut down, no further action is needed.
        }
    }

    /**
     * @return the most recent metrics snapshot from the collector.
     * This method can be used for on-demand metrics queries.
     */
    public MetricsSnapshot getMetricsSnapshot() {
        return metricsCollector.getSnapshot();
    }
}