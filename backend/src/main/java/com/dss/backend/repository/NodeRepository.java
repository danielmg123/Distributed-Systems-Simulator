package com.dss.backend.repository;

import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * {@code NodeRepository} manages persistence and retrieval of {@link Node} entities
 * from MongoDB.
 *
 * <p>This interface inherits common CRUD methods from {@link MongoRepository}.
 * Below are custom finder methods specific to the Node domain:</p>
 * <ul>
 *   <li>{@link #findByStatus(NodeStatus)} – Retrieves a list of nodes filtered by a given status
 *       (e.g. ACTIVE, FAILED, INACTIVE).</li>
 *   <li>{@link #findByAddress(String)} – Locates a node by its unique address field.</li>
 * </ul>
 */
public interface NodeRepository extends MongoRepository<Node, String> {

    /**
     * Finds all nodes matching a given status.
     *
     * @param status the node status (e.g. ACTIVE or FAILED)
     * @return a list of {@link Node} objects with the matching status
     */
    List<Node> findByStatus(NodeStatus status);

    /**
     * Finds a single node by its address field.
     *
     * @param address the unique network address of the node
     * @return the {@link Node} that has this address, or null if none found
     */
    Node findByAddress(String address);
}