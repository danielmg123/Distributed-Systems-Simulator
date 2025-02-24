package com.dss.backend.service;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.algorithm.consensus.ConsensusAlgorithmFactory;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.SimulationEngine;
import com.dss.backend.engine.concurrent.TopologyPlacer;
import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.model.Node;
import com.dss.backend.model.Simulation;
import com.dss.backend.model.SimulationStatus;
import com.dss.backend.repository.NodeRepository;
import com.dss.backend.repository.SimulationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SimulationService {

    @Autowired
    private SimulationRepository simulationRepository;

    @Autowired
    private NodeRepository nodeRepository;

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
        // 1. Load Simulation from DB
        Simulation simulation = simulationRepository.findById(simulationId)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found"));

        // Log simulation configuration if provided
        if (simulation.getConfig() != null) {
            System.out.println("Running simulation with config: " + simulation.getConfig());
        } else {
            System.out.println("No simulation configuration provided; defaulting to Paxos.");
        }

        // 2. Retrieve the list of Node objects.
        List<Node> nodes = nodeRepository.findAll();

        // 3. Use the ConsensusAlgorithmFactory to create an algorithm instance
        MessageRouter router = new MessageRouter();
        ConsensusAlgorithm algorithm = ConsensusAlgorithmFactory.createAlgorithm(
                "node0", // For example, a hard-coded node ID
                getAllNodeIds(nodes),
                simulation.getConfig(), // Pass the simulation configuration (which includes algorithm type etc...)
                router);

        // 4. Create and configure the SimulationEngine.
        SimulationEngine engine = new SimulationEngine();
        engine.initializeNodes(nodes, algorithm);
        engines.put(simulationId, engine);

        // 5. Optionally, assign network topology if defined in the simulation config.
        if (simulation.getConfig() != null && simulation.getConfig().getTopologyType() != null) {
            Map<String, List<String>> neighborMapping = TopologyPlacer.assignNeighbors(
                    simulation.getConfig().getTopologyType(), nodes);
            System.out.println("Computed neighbor mapping: " + neighborMapping);
            // You may want to store this mapping somewhere or pass it to nodes.
        }

        // 6. Update simulation status to RUNNING.
        simulation.setStatus(SimulationStatus.RUNNING);
        simulationRepository.save(simulation);

        // 7. Start the simulation.
        engine.startSimulation();

        // 8. Optionally, start failure simulation based on config settings.
        if (simulation.getConfig() != null) {
            double failurePercentage = simulation.getConfig().getFailurePercentage();
            if (failurePercentage > 0) {
                // Check failure status every 5 seconds (5000 milliseconds)
                engine.startFailureSimulation(failurePercentage, 5000);
                System.out.println("Started failure simulation with " + failurePercentage + "% failure rate.");
            }
        }
    }


    private List<String> getAllNodeIds(List<Node> nodes) {
        return nodes.stream().map(Node::getId).toList();
    }

    public void failNode(String simulationId, String nodeId) {
        SimulationEngine engine = engines.get(simulationId);
        if (engine != null) {
            engine.failNode(nodeId);
        }
    }

    public void stopSimulation(String simulationId) {
        SimulationEngine engine = engines.get(simulationId);
        if (engine != null) {
            engine.stopSimulation();
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