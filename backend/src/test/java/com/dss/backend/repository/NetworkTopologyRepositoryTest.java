package com.dss.backend.repository;

import com.dss.backend.model.NetworkTopology;
import com.dss.backend.model.TopologyType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DataMongoTest
public class NetworkTopologyRepositoryTest {

    @Autowired
    private NetworkTopologyRepository networkTopologyRepository;

    @Test
    public void testFindByType() {
        // Clean up the repository.
        networkTopologyRepository.deleteAll();

        // Insert topologies with different types.
        NetworkTopology topo1 = new NetworkTopology();
        topo1.setType(TopologyType.MESH);
        networkTopologyRepository.save(topo1);

        NetworkTopology topo2 = new NetworkTopology();
        topo2.setType(TopologyType.RING);
        networkTopologyRepository.save(topo2);

        NetworkTopology topo3 = new NetworkTopology();
        topo3.setType(TopologyType.MESH);
        networkTopologyRepository.save(topo3);

        // Query for topologies of type MESH.
        List<NetworkTopology> meshTopologies = networkTopologyRepository.findByType(TopologyType.MESH);
        assertNotNull(meshTopologies);
        assertEquals(2, meshTopologies.size());
        meshTopologies.forEach(topology -> assertEquals(TopologyType.MESH, topology.getType()));
    }
}