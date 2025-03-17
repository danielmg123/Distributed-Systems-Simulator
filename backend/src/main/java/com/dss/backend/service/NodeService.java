package com.dss.backend.service;

import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.model.Node;
import com.dss.backend.repository.NodeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * The {@code NodeService} handles business logic around {@link Node} entities,
 * which represent the simulator's nodes in the database.
 * </p>
 *
 * <p>
 * <strong>Business Logic & Responsibilities:</strong>
 * <ul>
 *   <li>Fetching the complete list of nodes for display or selection in the simulator.</li>
 *   <li>Creating and saving new nodes (e.g., adding them to the simulation environment).</li>
 *   <li>Deleting nodes (removing them from the simulation environment).</li>
 *   <li>Ensuring that a node must exist before deletion or retrieval,
 *       throwing an appropriate exception if not found.</li>
 * </ul>
 * </p>
 *
 * <p>
 * <strong>Side Effects:</strong>
 * <ul>
 *   <li>Any create, update, or delete operations here reflect changes to the underlying MongoDB
 *       collection storing node data.</li>
 * </ul>
 * </p>
 */
@Service
public class NodeService {

    @Autowired
    private NodeRepository nodeRepository;

    /**
     * <p>Retrieves all {@code Node} objects from the database.</p>
     *
     * @return a {@link List} of all stored nodes.
     */
    public List<Node> getAllNodes() {
        return nodeRepository.findAll();
    }

    /**
     * <p>Finds a {@code Node} by its unique identifier. If no node is found,
     * a {@link ResourceNotFoundException} is thrown.</p>
     *
     * @param id the unique ID of the node in the MongoDB collection.
     * @return the {@link Node} matching the given {@code id}.
     * @throws ResourceNotFoundException if no node with the specified {@code id} exists.
     */
    public Node getNodeByIdOrThrow(String id) {
        return nodeRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Node not found with id: " + id));
    }

    /**
     * <p>Saves (creates or updates) the given {@link Node} in the database.
     * If it has an existing ID, it updates; otherwise, it creates a new record.</p>
     *
     * <p><strong>Side Effect:</strong> This operation writes to the MongoDB, either
     * inserting a new node or overwriting an existing one (depending on ID presence).</p>
     *
     * @param node the {@link Node} to be saved.
     * @return the newly saved or updated {@link Node} instance, containing an assigned
     *         ID if it was newly created.
     */
    public Node saveNode(Node node) {
        return nodeRepository.save(node);
    }

    /**
     * <p>Deletes a node from the database.</p>
     * <ul>
     *   <li>Checks existence first via {@link #getNodeByIdOrThrow(String)}</li>
     *   <li>If found, deletes the record in MongoDB.</li>
     *   <li>Throws a {@link ResourceNotFoundException} if the node does not exist.</li>
     * </ul>
     *
     * @param id the unique ID of the node to delete.
     */
    public void deleteNode(String id) {
        // Attempt to find the node or throw an exception if it doesn't exist
        nodeRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException("Node not found with id: " + id));
        // If the node is present, proceed with deletion
        nodeRepository.deleteById(id);
    }
}