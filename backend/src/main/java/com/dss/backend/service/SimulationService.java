package com.dss.backend.service;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.ConsensusAlgorithmFactory;
import com.dss.backend.controller.SimulationWebSocketController;
import com.dss.backend.dto.EventDTO;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.SimulationEngine;
import com.dss.backend.engine.concurrent.TopologyPlacer;
import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.model.*;
import com.dss.backend.repository.NodeRepository;
import com.dss.backend.repository.SimulationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SimulationService {

    private static final Logger logger = LoggerFactory.getLogger(SimulationService.class);

    @Autowired
    private SimulationRepository simulationRepository;

    @Autowired
    private NodeRepository nodeRepository;

    @Autowired
    private SimulationWebSocketController simulationWebSocketController;


    // Map to hold running simulation engines (keyed by simulation ID)
    private final Map<String, SimulationEngine> engines = new ConcurrentHashMap<>();

    public List<Simulation> getAllSimulations() {
        return simulationRepository.findAll();
    }

    public Simulation getSimulationByIdOrThrow(String id) {
        return simulationRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + id));
    }

    public Simulation saveSimulation(Simulation simulation) {
        // The Simulation object includes an embedded SimulationConfig (if provided)
        return simulationRepository.save(simulation);
    }

    public void deleteSimulation(String id) {
        simulationRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + id));
        simulationRepository.deleteById(id);
    }

    public void runSimulation(String simulationId) {
        // 1. Retrieve the simulation from the database or throw an exception if not found
        Simulation simulation = simulationRepository.findById(simulationId)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found"));

        // 2. Retrieve the list of nodes that will participate in the simulation
        List<Node> nodes = nodeRepository.findAll();

        // 3. Create a message router for communication between nodes
        MessageRouter router = new MessageRouter();

        // 4. Create and configure the SimulationEngine, injecting the WebSocketController
        SimulationEngine engine = new SimulationEngine(simulationWebSocketController);
        engine.initializeNodes(nodes, simulation.getConfig(), simulation.getConfig().getTopologyType());

        // Store the engine in the map of running simulations
        engines.put(simulationId, engine);

        // 5. Set up network topology if defined in the simulation config
        if (simulation.getConfig() != null && simulation.getConfig().getTopologyType() != null) {
            Map<String, List<String>> neighborMapping = TopologyPlacer.assignNeighbors(
                    simulation.getConfig().getTopologyType(), nodes);
            logger.info("Computed neighbor mapping: {}", neighborMapping);
        }

        // 6. Update the simulation status to RUNNING and save it to the database
        simulation.setStatus(SimulationStatus.RUNNING);
        simulationRepository.save(simulation);

        // 7. Start the simulation engine
        engine.startSimulation(simulationId);

        // 8. Start sending real-time updates (metrics & events) via WebSocket
        engine.startMetricsUpdates(simulationId);

        // 9. Log and broadcast an event indicating that the simulation has started
        Event startEvent = new Event();
        startEvent.setType(EventType.SIMULATION_STARTED);
        startEvent.setDetails("Simulation has started.");
        startEvent.setTimestamp(LocalDateTime.now());

        // Send WebSocket event update
        simulationWebSocketController.sendEventUpdate(simulationId, mapEventToDTO(startEvent));

        // Persist the event in MongoDB
        logEvent(simulationId, startEvent);

        // 10. Optionally, start failure simulation if configured
        if (simulation.getConfig() != null) {
            double failurePercentage = simulation.getConfig().getFailurePercentage();
            if (failurePercentage > 0) {
                // Start failure simulation with the given failure rate
                engine.startFailureSimulation(simulationId, failurePercentage, 5000);
                logger.info("Started failure simulation with {}% failure rate.", failurePercentage);

                // 12. Log and broadcast an event indicating failure simulation has started
                Event failureEvent = new Event();
                failureEvent.setType(EventType.FAILURE_SIMULATION_STARTED);
                failureEvent.setDetails("Failure simulation started with " + failurePercentage + "% failure rate.");
                failureEvent.setTimestamp(LocalDateTime.now());

                // Send WebSocket event update
                simulationWebSocketController.sendEventUpdate(simulationId, mapEventToDTO(failureEvent));

                // Persist the event in MongoDB
                logEvent(simulationId, failureEvent);
            }
        }
    }

    private List<String> getAllNodeIds(List<Node> nodes) {
        return nodes.stream().map(Node::getId).toList();
    }

    public void failNode(String simulationId, String nodeId) {
        SimulationEngine engine = engines.get(simulationId);
        if (engine != null) {
            engine.failNode(simulationId, nodeId);
        }
    }

    public void logEvent(String simulationId, Event event) {
        Simulation simulation = getSimulationByIdOrThrow(simulationId);
        if (simulation.getEvents() == null) {
            simulation.setEvents(new ArrayList<>());
        }
        simulation.getEvents().add(event);
        simulationRepository.save(simulation);
    }

    private EventDTO mapEventToDTO(Event event) {
        EventDTO dto = new EventDTO();
        dto.setType(event.getType());
        dto.setDetails(event.getDetails());
        dto.setTimestamp(event.getTimestamp());
        return dto;
    }


    public void stopSimulation(String simulationId) {
        SimulationEngine engine = engines.get(simulationId);
        if (engine != null) {
            engine.stopSimulation(simulationId);
            engines.remove(simulationId);

            // Optionally update simulation status to COMPLETED
            Simulation sim = simulationRepository.findById(simulationId)
                    .orElseThrow(() -> new ResourceNotFoundException("Simulation not found"));
            sim.setStatus(SimulationStatus.COMPLETED);
            simulationRepository.save(sim);
        }
    }

    // Retrieves the metrics snapshot for a given simulation
    public MetricsSnapshot getSimulationMetrics(String simulationId) {
        SimulationEngine engine = engines.get(simulationId);
        if (engine == null) {
            throw new ResourceNotFoundException("Simulation not found or not running for id: " + simulationId);
        }
        return engine.getMetricsSnapshot();
    }
}