package com.dss.backend.controller;

import com.dss.backend.model.NetworkTopology;
import com.dss.backend.service.NetworkTopologyService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/topologies")
@Tag(name = "Topology Controller", description = "Endpoints for managing network topologies")
public class TopologyController {

    @Autowired
    private NetworkTopologyService networkTopologyService;

    @Operation(
            summary = "Get All Topologies",
            description = "Retrieves all network topologies configured in the system."
    )
    @GetMapping
    public List<NetworkTopology> getAllTopologies() {
        return networkTopologyService.getAllTopologies();
    }

    @Operation(
            summary = "Get Topology by ID",
            description = "Retrieves a specific network topology by its unique identifier."
    )
    @GetMapping("/{id}")
    public ResponseEntity<NetworkTopology> getTopologyById(@PathVariable String id) {
        NetworkTopology topology = networkTopologyService.getTopologyByIdOrThrow(id);
        return ResponseEntity.ok(topology);
    }

    @Operation(
            summary = "Create Topology",
            description = "Creates a new network topology based on the provided details."
    )
    @PostMapping
    public NetworkTopology createTopology(@RequestBody NetworkTopology topology) {
        return networkTopologyService.saveTopology(topology);
    }

    @Operation(
            summary = "Delete Topology",
            description = "Deletes a network topology by its unique identifier."
    )
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteTopology(@PathVariable String id) {
        networkTopologyService.deleteTopology(id);
        return ResponseEntity.ok().build();
    }
}