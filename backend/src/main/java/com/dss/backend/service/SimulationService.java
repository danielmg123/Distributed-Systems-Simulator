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
        simulationRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException("Simulation not found with id: " + id));
        simulationRepository.deleteById(id);
    }

    private final Map<String, SimulationEngine> engines = new ConcurrentHashMap<>();
    
    public void runSimulation(String simulationId) {
        // 1. Load Simulation from DB
        Simulation simulation = simulationRepository.findById(simulationId)
            .orElseThrow(() -> new ResourceNotFoundException("Simulation not found"));
        
        // 2. Retrieve the list of Node objects (or however you store them).
        //    For example, if your simulation references a topology or set of node IDs,
        //    you’ll need to load them from the NodeRepository.
        List<Node> nodes = nodeRepository.findAll();
        
        // 3. Pick or build the algorithm. Right now, Paxos is hard-coded:
        PaxosAlgorithm algorithm = new PaxosAlgorithm(
            "node0",      // you might assign a real ID for each node
            getAllNodeIds(nodes),
            new MessageRouter() // or pass an existing router if needed
        );

        // 4. Create and configure SimulationEngine
        SimulationEngine engine = new SimulationEngine();
        engine.initializeNodes(nodes, algorithm);
        engines.put(simulationId, engine);

        // 5. Optionally update simulation status to RUNNING
        simulation.setStatus(SimulationStatus.RUNNING);
        simulationRepository.save(simulation);

        // 6. Start the simulation
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

            // Optionally update simulation status
            Simulation sim = simulationRepository.findById(simulationId)
                .orElseThrow(() -> new ResourceNotFoundException("Simulation not found"));
            sim.setStatus(SimulationStatus.COMPLETED);
            simulationRepository.save(sim);
        }
    }
}

