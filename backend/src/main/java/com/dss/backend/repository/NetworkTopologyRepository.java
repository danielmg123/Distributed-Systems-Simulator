package com.dss.backend.repository;

import com.dss.backend.model.NetworkTopology;
import com.dss.backend.model.TopologyType;
import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface NetworkTopologyRepository extends MongoRepository<NetworkTopology, String> {
        List<NetworkTopology> findByType(TopologyType type);

    // Assuming I have a reference or embedded Node IDs or names
    // NetworkTopology findByNodesId(String nodeId);
}

