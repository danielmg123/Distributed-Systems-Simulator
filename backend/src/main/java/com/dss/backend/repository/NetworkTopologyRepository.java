package com.dss.backend.repository;

import com.dss.backend.model.NetworkTopology;
import com.dss.backend.model.TopologyType;
import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * {@code NetworkTopologyRepository} manages {@link NetworkTopology} documents in MongoDB.
 *
 * <p>In addition to standard CRUD methods, it defines:</p>
 * <ul>
 *   <li>{@link #findByType(TopologyType)} – Retrieves all stored topologies of a
 *       specified {@link TopologyType} (e.g. MESH, RING, etc.).</li>
 * </ul>
 */
public interface NetworkTopologyRepository extends MongoRepository<NetworkTopology, String> {

        /**
         * Finds all network topologies of the given type.
         *
         * @param type the type of topology (e.g., MESH, RING, STAR)
         * @return a list of matching {@link NetworkTopology} objects
         */
        List<NetworkTopology> findByType(TopologyType type);
}