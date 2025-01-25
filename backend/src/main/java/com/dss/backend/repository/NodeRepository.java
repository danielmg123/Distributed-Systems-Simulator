package com.dss.backend.repository;

import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface NodeRepository extends MongoRepository<Node, String> {
    
    List<Node> findByStatus(NodeStatus status);

    Node findByAddress(String address);
}