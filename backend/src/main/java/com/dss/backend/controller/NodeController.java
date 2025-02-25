package com.dss.backend.controller;

import com.dss.backend.dto.NodeDTO;
import com.dss.backend.mapper.NodeMapper;
import com.dss.backend.model.Node;
import com.dss.backend.service.NodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/nodes")
public class NodeController {

    @Autowired
    private NodeService nodeService;

    @Autowired
    private NodeMapper nodeMapper;

    @GetMapping
    public List<NodeDTO> getAllNodes() {
        List<Node> nodes = nodeService.getAllNodes();
        return nodes.stream()
                .map(nodeMapper::nodeToNodeDTO)
                .collect(Collectors.toList());
    }

    @GetMapping("/{id}")
    public ResponseEntity<NodeDTO> getNodeById(@PathVariable String id) {
        Node node = nodeService.getNodeByIdOrThrow(id);
        return ResponseEntity.ok(nodeMapper.nodeToNodeDTO(node));
    }

    @PostMapping
    public NodeDTO createNode(@RequestBody NodeDTO nodeDTO) {
        Node node = nodeMapper.nodeDTOToNode(nodeDTO);
        Node saved = nodeService.saveNode(node);
        return nodeMapper.nodeToNodeDTO(saved);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteNode(@PathVariable String id) {
        nodeService.deleteNode(id);
        return ResponseEntity.ok().build();
    }
}
