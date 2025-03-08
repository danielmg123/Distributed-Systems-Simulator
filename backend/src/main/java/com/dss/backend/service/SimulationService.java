package com.dss.backend.service;

import com.dss.backend.controller.SimulationWebSocketController;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;
import com.dss.backend.model.Event;
import com.dss.backend.model.EventType;
import com.dss.backend.model.Node;
import com.dss.backend.model.Simulation;
import com.dss.backend.model.SimulationStatus;
import com.dss.backend.repository.NodeRepository;
import com.dss.backend.repository.SimulationRepository;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.service.engine.NodeInitializationService;
import com.dss.backend.service.engine.MetricsUpdateService;
import com.dss.backend.service.engine.EventLoggerService;
import com.dss.backend.service.engine.SimulationOrchestrator;
import com.dss.backend.consensus.ConsensusAlgorithmFactory;
import com.dss.backend.metrics.DefaultMetricsCollector;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.metrics.PerformanceMetricsCollector;
import com.dss.backend.config.SimulationProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Service
public class SimulationService {

    private final AppLogger appLogger = new DefaultAppLogger(SimulationService.class);

    @Autowired
    private SimulationRepository simulationRepository;

    @Autowired
    private NodeRepository nodeRepository;

    @Autowired
    private SimulationWebSocketController simulationWebSocketController;

    // Use the scheduler bean provided by AppConfig
    @Autowired
    private Scheduler scheduler;

    // Inject simulation properties to pass to child services.
    @Autowired
    private SimulationProperties simulationProperties;

    // Map of simulation IDs to their respective orchestrators.
    private final Map<String, SimulationOrchestrator> orchestrators = new ConcurrentHashMap<>();

    // Shared metrics collector instance.
    private final PerformanceMetricsCollector metricsCollector = new DefaultMetricsCollector();

    public List<Simulation> getAllSimulations() {
        return simulationRepository.findAll();
    }

    public Simulation getSimulationByIdOrThrow(String id) {
        return simulationRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + id));
    }

    public Simulation saveSimulation(Simulation simulation) {
        return simulationRepository.save(simulation);
    }

    public void deleteSimulation(String id) {
        simulationRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + id));
        simulationRepository.deleteById(id);
    }

    public void runSimulation(String simulationId) {
        // 1. Retrieve the simulation.
        Simulation simulation = simulationRepository.findById(simulationId)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + simulationId));

        // 2. Retrieve all nodes participating.
        List<Node> nodes = nodeRepository.findAll();

        // 3. Create a shared MessageRouter.
        MessageRouter router = new MessageRouter();

        // 4. Instantiate services using injected scheduler and properties.
        ConsensusAlgorithmFactory consensusFactory = new ConsensusAlgorithmFactory(router, scheduler, simulationProperties);
        NodeInitializationService nodeInitService = new NodeInitializationService(router, scheduler, consensusFactory, simulationProperties);
        MetricsUpdateService metricsUpdateService = new MetricsUpdateService(metricsCollector, simulationWebSocketController, scheduler);
        EventLoggerService eventLoggerService = new EventLoggerService(simulationWebSocketController);

        // 5. Create the SimulationOrchestrator.
        SimulationOrchestrator orchestrator = new SimulationOrchestrator(
                router,
                scheduler,
                nodeInitService,
                metricsUpdateService,
                eventLoggerService
        );

        // 6. Initialize simulation nodes.
        orchestrator.initializeSimulationNodes(nodes, simulation.getConfig(), simulation.getConfig().getTopologyType());

        // 7. Store the orchestrator.
        orchestrators.put(simulationId, orchestrator);

        // 8. Compute and log topology mapping if applicable.
        if (simulation.getConfig() != null && simulation.getConfig().getTopologyType() != null) {
            Map<String, List<String>> neighborMapping = orchestrator.computeTopologyMapping(nodes, simulation.getConfig().getTopologyType());
            appLogger.info("Computed neighbor mapping: {}", neighborMapping);
        }

        // 9. Update simulation status to RUNNING.
        simulation.setStatus(SimulationStatus.RUNNING);
        simulationRepository.save(simulation);

        // 10. Start simulation orchestration.
        orchestrator.startSimulation(simulationId);

        // 11. Log and broadcast a "simulation started" event.
        Event startEvent = new Event();
        startEvent.setType(EventType.SIMULATION_STARTED);
        startEvent.setDetails("Simulation has started.");
        startEvent.setTimestamp(LocalDateTime.now());
        simulationWebSocketController.sendEventUpdate(simulationId, eventLoggerService.mapEventToDTO(startEvent));
        eventLoggerService.logEvent(simulationId, startEvent.getDetails(), startEvent.getType());

        // 12. Optionally, if a failure percentage is specified, start failure simulation.
        if (simulation.getConfig() != null) {
            double failurePercentage = simulation.getConfig().getFailurePercentage();
            if (failurePercentage > 0) {
                // Here we use a fixed interval (5000 ms) but this could be externalized as well.
                orchestrator.startFailureSimulation(simulationId, failurePercentage, 5000);
                appLogger.info("Started failure simulation with {}% failure rate.", failurePercentage);

                Event failureEvent = new Event();
                failureEvent.setType(EventType.FAILURE_SIMULATION_STARTED);
                failureEvent.setDetails("Failure simulation started with " + failurePercentage + "% failure rate.");
                failureEvent.setTimestamp(LocalDateTime.now());
                simulationWebSocketController.sendEventUpdate(simulationId, eventLoggerService.mapEventToDTO(failureEvent));
                eventLoggerService.logEvent(simulationId, failureEvent.getDetails(), failureEvent.getType());
            }
        }
    }

    public void stopSimulation(String simulationId) {
        SimulationOrchestrator orchestrator = orchestrators.get(simulationId);
        if (orchestrator != null) {
            orchestrator.stopSimulation(simulationId);
            orchestrators.remove(simulationId);
            Simulation simulation = simulationRepository.findById(simulationId)
                    .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + simulationId));
            simulation.setStatus(SimulationStatus.COMPLETED);
            simulationRepository.save(simulation);
        }
    }

    public void failNode(String simulationId, String nodeId) {
        SimulationOrchestrator orchestrator = orchestrators.get(simulationId);
        if (orchestrator != null) {
            orchestrator.failNode(simulationId, nodeId);
        }
    }

    public MetricsSnapshot getSimulationMetrics(String simulationId) {
        // Try to retrieve the orchestrator for the given simulation.
        SimulationOrchestrator orchestrator = orchestrators.get(simulationId);
        if (orchestrator == null) {
            // If not found (e.g., simulation is stopped), return the snapshot from the shared metrics collector.
            // This change ensures that we always return a (possibly default) snapshot.
            return metricsCollector.getSnapshot();
        }
        return orchestrator.getMetricsSnapshot();
    }

}