package com.dss.backend.service;

import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.model.Node;
import com.dss.backend.repository.NodeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class NodeService {

    @Autowired
    private NodeRepository nodeRepository;

    public List<Node> getAllNodes() {
        return nodeRepository.findAll();
    }

    public Node getNodeByIdOrThrow(String id) {
        return nodeRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Node not found with id: " + id));
    }

    public Node saveNode(Node node) {
        return nodeRepository.save(node);
    }

    public void deleteNode(String id) {
        nodeRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException("Node not found with id: " + id));
        nodeRepository.deleteById(id);
    }
}
