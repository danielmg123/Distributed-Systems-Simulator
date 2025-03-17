package com.dss.backend.service;

import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.model.NetworkTopology;
import com.dss.backend.repository.NetworkTopologyRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * The {@code NetworkTopologyService} manages {@link NetworkTopology} entities, which
 * represent different ways nodes can be arranged (mesh, ring, etc.).
 * </p>
 *
 * <p>
 * <strong>Business Logic & Responsibilities:</strong>
 * <ul>
 *   <li>Retrieves all topologies for UI display or simulator usage.</li>
 *   <li>Finds a specific topology by ID, ensuring it exists.</li>
 *   <li>Creates or updates topologies with the chosen arrangement of nodes.</li>
 *   <li>Deletes a topology when it is no longer needed.</li>
 * </ul>
 * </p>
 *
 * <p>
 * <strong>Side Effects:</strong>
 * <ul>
 *   <li>Write operations create, update, or remove topology data in MongoDB.</li>
 * </ul>
 * </p>
 */
@Service
public class NetworkTopologyService {

    @Autowired
    private NetworkTopologyRepository networkTopologyRepository;

    /**
     * <p>Retrieves all available {@link NetworkTopology} records from the database.</p>
     *
     * @return a list of all stored topologies.
     */
    public List<NetworkTopology> getAllTopologies() {
        return networkTopologyRepository.findAll();
    }

    /**
     * <p>Locates a single {@code NetworkTopology} by ID or throws
     * a {@link ResourceNotFoundException} if none is found.</p>
     *
     * @param id the unique ID of the desired topology.
     * @return the topology record if found.
     * @throws ResourceNotFoundException if no topology with the given ID exists.
     */
    public NetworkTopology getTopologyByIdOrThrow(String id) {
        return networkTopologyRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("NetworkTopology not found with id: " + id));
    }

    /**
     * <p>Saves a new or existing topology to the database.
     * If the topology’s {@code id} is null or empty, it is inserted as new;
     * otherwise it updates an existing record.</p>
     *
     * @param topology the topology definition to save or update.
     * @return the saved instance, including any generated ID for new records.
     */
    public NetworkTopology saveTopology(NetworkTopology topology) {
        return networkTopologyRepository.save(topology);
    }

    /**
     * <p>Deletes the specified topology, provided it exists.</p>
     * <ul>
     *   <li>If not found, throws a {@link ResourceNotFoundException}.</li>
     *   <li>If found, the record is removed from the database.</li>
     * </ul>
     *
     * @param id the unique ID of the topology to delete.
     */
    public void deleteTopology(String id) {
        // Ensure topology exists first
        networkTopologyRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException("NetworkTopology not found with id: " + id));
        networkTopologyRepository.deleteById(id);
    }
}