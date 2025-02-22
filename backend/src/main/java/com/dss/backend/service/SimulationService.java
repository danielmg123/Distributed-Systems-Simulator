package com.dss.backend.service;

import com.dss.backend.algorithm.consensus.paxos.PaxosAlgorithm;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.SimulationEngine;
import com.dss.backend.exception.ResourceNotFoundException;
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
        // The Simulation object now includes an embedded SimulationConfig (if provided)
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

        // (Optional) Log simulation configuration if provided
        if (simulation.getConfig() != null) {
            System.out.println("Running simulation with config: " + simulation.getConfig());
        } else {
            System.out.println("No simulation configuration provided.");
        }

        // 2. Retrieve the list of Node objects.
        List<Node> nodes = nodeRepository.findAll();

        // 3. Build the consensus algorithm instance (hard-coded to Paxos in this example).
        PaxosAlgorithm algorithm = new PaxosAlgorithm(
                "node0",                 // For example, a hard-coded node ID (adjust as needed)
                getAllNodeIds(nodes),
                new MessageRouter()      // Create a new MessageRouter instance
        );

        // 4. Create and configure the SimulationEngine.
        SimulationEngine engine = new SimulationEngine();
        engine.initializeNodes(nodes, algorithm);
        engines.put(simulationId, engine);

        // 5. Update simulation status to RUNNING.
        simulation.setStatus(SimulationStatus.RUNNING);
        simulationRepository.save(simulation);

        // 6. Start the simulation.
        engine.startSimulation();
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
}
