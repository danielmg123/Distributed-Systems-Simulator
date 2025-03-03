package com.dss.backend.controller;

import com.dss.backend.dto.EventDTO;
import com.dss.backend.dto.SimulationDTO;
import com.dss.backend.mapper.EventMapper;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.mapper.SimulationMapper;
import com.dss.backend.model.Simulation;
import com.dss.backend.service.SimulationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/simulations")
public class SimulationController {

    @Autowired
    private SimulationService simulationService;

    @Autowired
    private SimulationMapper simulationMapper;

    @Autowired
    private EventMapper eventMapper;

    @GetMapping
    public List<SimulationDTO> getAllSimulations() {
        List<Simulation> simulations = simulationService.getAllSimulations();
        return simulations.stream()
                .map(simulationMapper::simulationToSimulationDTO)
                .collect(Collectors.toList());
    }

    @GetMapping("/{id}")
    public ResponseEntity<SimulationDTO> getSimulationById(@PathVariable String id) {
        Simulation simulation = simulationService.getSimulationByIdOrThrow(id);
        return ResponseEntity.ok(simulationMapper.simulationToSimulationDTO(simulation));
    }

    @PostMapping
    public SimulationDTO createSimulation(@RequestBody SimulationDTO simulationDTO) {
        Simulation simulation = simulationMapper.simulationDTOToSimulation(simulationDTO);
        Simulation saved = simulationService.saveSimulation(simulation);
        return simulationMapper.simulationToSimulationDTO(saved);
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

    @GetMapping("/{id}/metrics")
    public ResponseEntity<MetricsSnapshot> getSimulationMetrics(@PathVariable String id) {
        MetricsSnapshot snapshot = simulationService.getSimulationMetrics(id);
        return ResponseEntity.ok(snapshot);
    }

    @GetMapping("/{id}/events")
    public ResponseEntity<List<EventDTO>> getSimulationEvents(@PathVariable String id) {
        Simulation simulation = simulationService.getSimulationByIdOrThrow(id);
        List<EventDTO> eventDTOs = simulation.getEvents().stream()
                .map(eventMapper::eventToEventDTO)
                .collect(Collectors.toList());
        return ResponseEntity.ok(eventDTOs);
    }
}