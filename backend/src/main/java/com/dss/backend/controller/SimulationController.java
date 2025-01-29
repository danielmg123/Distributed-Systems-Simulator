package com.dss.backend.controller;

import com.dss.backend.model.Simulation;
import com.dss.backend.service.SimulationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/simulations")
public class SimulationController {

    @Autowired
    private SimulationService simulationService;

    @GetMapping
    public List<Simulation> getAllSimulations() {
        return simulationService.getAllSimulations();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Simulation> getSimulationById(@PathVariable String id) {
        Simulation simulation = simulationService.getSimulationByIdOrThrow(id);
        return ResponseEntity.ok(simulation);
    }

    @PostMapping
    public Simulation createSimulation(@RequestBody Simulation simulation) {
        return simulationService.saveSimulation(simulation);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteSimulation(@PathVariable String id) {
        simulationService.deleteSimulation(id);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{id}/run")
    public ResponseEntity<String> runSimulation(@PathVariable String id) {
        simulationService.runSimulation(id);
        return ResponseEntity.ok("Simulation started with ID: " + id);
    }

    @PostMapping("/{id}/failNode/{nodeId}")
    public ResponseEntity<String> failNode(@PathVariable String id, @PathVariable String nodeId) {
        simulationService.failNode(id, nodeId);
        return ResponseEntity.ok("Node " + nodeId + " failed in simulation " + id);
    }

    @PostMapping("/{id}/stop")
    public ResponseEntity<String> stopSimulation(@PathVariable String id) {
        simulationService.stopSimulation(id);
        return ResponseEntity.ok("Simulation stopped for ID: " + id);
    }
}
