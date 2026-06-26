package com.dss.backend.controller;

import com.dss.backend.dto.EventDTO;
import com.dss.backend.dto.SimulationDTO;
import com.dss.backend.mapper.EventMapper;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.mapper.SimulationMapper;
import com.dss.backend.model.Simulation;
import com.dss.backend.service.SimulationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/simulations")
@Tag(name = "Simulation Controller", description = "Endpoints for managing simulations")
public class SimulationController {

    @Autowired
    private SimulationService simulationService;

    @Autowired
    private SimulationMapper simulationMapper;

    @Autowired
    private EventMapper eventMapper;

    @Operation(
            summary = "Get All Simulations",
            description = "Retrieves a list of all simulations currently stored."
    )
    @GetMapping
    public List<SimulationDTO> getAllSimulations() {
        List<Simulation> simulations = simulationService.getAllSimulations();
        return simulations.stream()
                .map(simulationMapper::simulationToSimulationDTO)
                .collect(Collectors.toList());
    }

    @Operation(
            summary = "Get Simulation by ID",
            description = "Retrieves a simulation by its unique identifier."
    )
    @GetMapping("/{id}")
    public ResponseEntity<SimulationDTO> getSimulationById(@PathVariable String id) {
        Simulation simulation = simulationService.getSimulationByIdOrThrow(id);
        return ResponseEntity.ok(simulationMapper.simulationToSimulationDTO(simulation));
    }

    @Operation(
            summary = "Create Simulation",
            description = "Creates a new simulation with the provided configuration."
    )
    @PostMapping
    public SimulationDTO createSimulation(@RequestBody SimulationDTO simulationDTO) {
        Simulation simulation = simulationMapper.simulationDTOToSimulation(simulationDTO);
        Simulation saved = simulationService.saveSimulation(simulation);
        return simulationMapper.simulationToSimulationDTO(saved);
    }

    @Operation(
            summary = "Delete Simulation",
            description = "Deletes a simulation based on its unique identifier."
    )
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteSimulation(@PathVariable String id) {
        simulationService.deleteSimulation(id);
        return ResponseEntity.ok().build();
    }

    @Operation(
            summary = "Run Simulation",
            description = "Starts a simulation for the given ID."
    )
    @PostMapping("/{id}/run")
    public ResponseEntity<String> runSimulation(@PathVariable String id) {
        simulationService.runSimulation(id);
        return ResponseEntity.ok("Simulation started with ID: " + id);
    }

    @Operation(
            summary = "Fail Node in Simulation",
            description = "Simulates a node failure within a simulation."
    )
    @PostMapping("/{id}/failNode/{nodeId}")
    public ResponseEntity<String> failNode(@PathVariable String id, @PathVariable String nodeId) {
        simulationService.failNode(id, nodeId);
        return ResponseEntity.ok("Node " + nodeId + " failed in simulation " + id);
    }

    @Operation(
            summary = "Stop Simulation",
            description = "Stops the simulation for the given ID."
    )
    @PostMapping("/{id}/stop")
    public ResponseEntity<String> stopSimulation(@PathVariable String id) {
        simulationService.stopSimulation(id);
        return ResponseEntity.ok("Simulation stopped for ID: " + id);
    }

    @Operation(
            summary = "Get Simulation Metrics",
            description = "Retrieves performance metrics for a specific simulation."
    )
    @GetMapping("/{id}/metrics")
    public ResponseEntity<MetricsSnapshot> getSimulationMetrics(@PathVariable String id) {
        MetricsSnapshot snapshot = simulationService.getSimulationMetrics(id);
        return ResponseEntity.ok(snapshot);
    }

    @Operation(
            summary = "Get Simulation Events",
            description = "Retrieves a list of events associated with a simulation."
    )
    @GetMapping("/{id}/events")
    public ResponseEntity<List<EventDTO>> getSimulationEvents(@PathVariable String id) {
        Simulation simulation = simulationService.getSimulationByIdOrThrow(id);
        List<EventDTO> eventDTOs = simulation.getEvents().stream()
                .map(eventMapper::eventToEventDTO)
                .collect(Collectors.toList());
        return ResponseEntity.ok(eventDTOs);
    }

    @Operation(
            summary = "Get Simulation Topology",
            description = "Retrieves the neighbor-adjacency map computed for this simulation's topology " +
                    "(RING/STAR/TREE/MESH), for a dashboard to render. This is visualization metadata only -- " +
                    "it does not constrain how messages are actually routed between nodes; every consensus " +
                    "algorithm's broadcast logic always reaches every node regardless of topology, since " +
                    "quorum-based protocols need full connectivity to function."
    )
    @GetMapping("/{id}/topology")
    public ResponseEntity<Map<String, List<String>>> getSimulationTopology(@PathVariable String id) {
        Map<String, List<String>> topologyMapping = simulationService.getTopologyMapping(id);
        return ResponseEntity.ok(topologyMapping);
    }
}