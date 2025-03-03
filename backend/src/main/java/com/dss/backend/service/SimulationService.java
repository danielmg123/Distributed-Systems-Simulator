package com.dss.backend.service;

import com.dss.backend.controller.SimulationWebSocketController;
import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.model.Event;
import com.dss.backend.model.EventType;
import com.dss.backend.model.Node;
import com.dss.backend.model.Simulation;
import com.dss.backend.model.SimulationStatus;
import com.dss.backend.model.TopologyType;
import com.dss.backend.repository.NodeRepository;
import com.dss.backend.repository.SimulationRepository;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.service.NodeInitializationService;
import com.dss.backend.engine.service.MetricsUpdateService;
import com.dss.backend.engine.service.EventLoggerService;
import com.dss.backend.engine.service.SimulationOrchestrator;
import com.dss.backend.algorithm.consensus.ConsensusAlgorithmFactory;
import com.dss.backend.metrics.DefaultMetricsCollector;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.metrics.PerformanceMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Service
public class SimulationService {

    private static final Logger logger = LoggerFactory.getLogger(SimulationService.class);

    @Autowired
    private SimulationRepository simulationRepository;

    @Autowired
    private NodeRepository nodeRepository;

    @Autowired
    private SimulationWebSocketController simulationWebSocketController;

    // Shared scheduler for simulation tasks.
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

    // Map of simulation ID to its orchestrator.
    private final Map<String, SimulationOrchestrator> orchestrators = new ConcurrentHashMap<>();

    // A shared metrics collector used by MetricsUpdateService.
    private final PerformanceMetricsCollector metricsCollector = new DefaultMetricsCollector();

    public List<Simulation> getAllSimulations() {
        return simulationRepository.findAll();
    }

    public Simulation getSimulationByIdOrThrow(String id) {
        return simulationRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + id));
    }

    public Simulation saveSimulation(Simulation simulation) {
        // The Simulation object includes an embedded SimulationConfig (if provided).
        return simulationRepository.save(simulation);
    }

    public void deleteSimulation(String id) {
        simulationRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + id));
        simulationRepository.deleteById(id);
    }

    public void runSimulation(String simulationId) {
        // 1. Retrieve the simulation from the repository.
        Simulation simulation = simulationRepository.findById(simulationId)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + simulationId));

        // 2. Retrieve all nodes participating in the simulation.
        List<Node> nodes = nodeRepository.findAll();

        // 3. Create a shared MessageRouter.
        MessageRouter router = new MessageRouter();

        // 4. Instantiate the new services.
        // Create a ConsensusAlgorithmFactory instance with the shared router and scheduler.
        ConsensusAlgorithmFactory consensusFactory = new ConsensusAlgorithmFactory(router, scheduler);
        // Now pass the factory into the NodeInitializationService.
        NodeInitializationService nodeInitService = new NodeInitializationService(router, scheduler, consensusFactory);
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

        // 6. Use the orchestrator to initialize simulation nodes.
        orchestrator.initializeSimulationNodes(nodes, simulation.getConfig(), simulation.getConfig().getTopologyType());

        // Store the orchestrator for later control.
        orchestrators.put(simulationId, orchestrator);

        // 7. (Optional) Compute the topology mapping and log it.
        if (simulation.getConfig() != null && simulation.getConfig().getTopologyType() != null) {
            Map<String, List<String>> neighborMapping = orchestrator.computeTopologyMapping(nodes, simulation.getConfig().getTopologyType());
            logger.info("Computed neighbor mapping: {}", neighborMapping);
        }

        // 8. Update the simulation status to RUNNING and persist.
        simulation.setStatus(SimulationStatus.RUNNING);
        simulationRepository.save(simulation);

        // 9. Start the simulation orchestration.
        orchestrator.startSimulation(simulationId);

        // 10. Log and broadcast a "simulation started" event.
        Event startEvent = new Event();
        startEvent.setType(EventType.SIMULATION_STARTED);
        startEvent.setDetails("Simulation has started.");
        startEvent.setTimestamp(LocalDateTime.now());
        simulationWebSocketController.sendEventUpdate(simulationId, eventLoggerService.mapEventToDTO(startEvent));
        eventLoggerService.logEvent(simulationId, startEvent.getDetails(), startEvent.getType());

        // 11. If configured, start failure simulation.
        if (simulation.getConfig() != null) {
            double failurePercentage = simulation.getConfig().getFailurePercentage();
            if (failurePercentage > 0) {
                orchestrator.startFailureSimulation(simulationId, failurePercentage, 5000);
                logger.info("Started failure simulation with {}% failure rate.", failurePercentage);

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
        SimulationOrchestrator orchestrator = orchestrators.get(simulationId);
        if (orchestrator == null) {
            throw new ResourceNotFoundException("Simulation not found or not running for id: " + simulationId);
        }
        return orchestrator.getMetricsSnapshot();
    }
}