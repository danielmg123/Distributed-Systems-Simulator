package com.dss.backend.controller;

import com.dss.backend.dto.NetworkConditionsDTO;
import com.dss.backend.dto.NodeDTO;
import com.dss.backend.dto.ProposeRequestDTO;
import com.dss.backend.dto.SimulationDTO;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.mapper.SimulationMapper;
import com.dss.backend.model.Simulation;
import com.dss.backend.service.SimulationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
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
    public SimulationDTO createSimulation(@Valid @RequestBody SimulationDTO simulationDTO) {
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
            summary = "Recover Node in Simulation",
            description = "Recovers a previously-failed node within a simulation."
    )
    @PostMapping("/{id}/recoverNode/{nodeId}")
    public ResponseEntity<String> recoverNode(@PathVariable String id, @PathVariable String nodeId) {
        simulationService.recoverNode(id, nodeId);
        return ResponseEntity.ok("Node " + nodeId + " recovered in simulation " + id);
    }

    @Operation(
            summary = "Propose Value",
            description = "Broadcasts a proposal to every active node in a running simulation. Each node's " +
                    "consensus algorithm decides whether to act on it (e.g. only the current Raft leader does) " +
                    "-- callers don't need to know which node, if any, is currently the leader."
    )
    @PostMapping("/{id}/propose")
    public ResponseEntity<String> propose(@PathVariable String id, @RequestBody ProposeRequestDTO request) {
        simulationService.propose(id, request.getValue());
        return ResponseEntity.ok("Value proposed in simulation " + id);
    }

    @Operation(
            summary = "Get Live Node Statuses",
            description = "Retrieves the live status and (protocol-specific) role of every node in a running " +
                    "simulation, for a dashboard's node grid. Unlike GET /api/nodes, which reads the static, " +
                    "persisted node pool, this reflects this specific simulation's actual running state."
    )
    @GetMapping("/{id}/nodes")
    public ResponseEntity<List<NodeDTO>> getNodeStatuses(@PathVariable String id) {
        return ResponseEntity.ok(simulationService.getNodeStatuses(id));
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

    @Operation(
            summary = "Update Network Conditions",
            description = "Live-updates a running simulation's simulated message loss rate and delivery " +
                    "delay range. Takes effect immediately for all subsequent message sends; has no effect " +
                    "if the simulation isn't currently running. Intended to back UI controls (e.g. sliders)."
    )
    @PutMapping("/{id}/network-conditions")
    public ResponseEntity<String> updateNetworkConditions(@PathVariable String id,
                                                          @RequestBody NetworkConditionsDTO conditions) {
        simulationService.updateNetworkConditions(
                id, conditions.getMessageLossRate(), conditions.getMinDelayMs(), conditions.getMaxDelayMs());
        return ResponseEntity.ok("Network conditions updated for simulation " + id);
    }
}