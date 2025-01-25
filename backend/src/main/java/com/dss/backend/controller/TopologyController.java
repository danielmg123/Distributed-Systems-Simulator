package com.dss.backend.controller;

import com.dss.backend.model.NetworkTopology;
import com.dss.backend.service.NetworkTopologyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/topologies")
public class TopologyController {

    @Autowired
    private NetworkTopologyService networkTopologyService;

    @GetMapping
    public List<NetworkTopology> getAllTopologies() {
        return networkTopologyService.getAllTopologies();
    }

    @GetMapping("/{id}")
    public ResponseEntity<NetworkTopology> getTopologyById(@PathVariable String id) {
        NetworkTopology topology = networkTopologyService.getTopologyByIdOrThrow(id);
        return ResponseEntity.ok(topology);
    }

    @PostMapping
    public NetworkTopology createTopology(@RequestBody NetworkTopology topology) {
        return networkTopologyService.saveTopology(topology);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteTopology(@PathVariable String id) {
        networkTopologyService.deleteTopology(id);
        return ResponseEntity.ok().build();
    }
}
