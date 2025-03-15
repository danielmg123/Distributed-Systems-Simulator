package com.dss.backend.controller;

import com.dss.backend.dto.NodeDTO;
import com.dss.backend.mapper.NodeMapper;
import com.dss.backend.model.Node;
import com.dss.backend.service.NodeService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/nodes")
@Tag(name = "Node Controller", description = "Endpoints for managing nodes")
public class NodeController {

    @Autowired
    private NodeService nodeService;

    @Autowired
    private NodeMapper nodeMapper;

    @Operation(
            summary = "Get All Nodes",
            description = "Retrieves all nodes registered in the system."
    )
    @GetMapping
    public List<NodeDTO> getAllNodes() {
        List<Node> nodes = nodeService.getAllNodes();
        return nodes.stream()
                .map(nodeMapper::nodeToNodeDTO)
                .collect(Collectors.toList());
    }

    @Operation(
            summary = "Get Node by ID",
            description = "Retrieves a node using its unique identifier."
    )
    @GetMapping("/{id}")
    public ResponseEntity<NodeDTO> getNodeById(@PathVariable String id) {
        Node node = nodeService.getNodeByIdOrThrow(id);
        return ResponseEntity.ok(nodeMapper.nodeToNodeDTO(node));
    }

    @Operation(
            summary = "Create Node",
            description = "Creates a new node based on the provided details."
    )
    @PostMapping
    public NodeDTO createNode(@RequestBody NodeDTO nodeDTO) {
        Node node = nodeMapper.nodeDTOToNode(nodeDTO);
        Node saved = nodeService.saveNode(node);
        return nodeMapper.nodeToNodeDTO(saved);
    }

    @Operation(
            summary = "Delete Node",
            description = "Deletes a node identified by its unique identifier."
    )
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteNode(@PathVariable String id) {
        nodeService.deleteNode(id);
        return ResponseEntity.ok().build();
    }
}