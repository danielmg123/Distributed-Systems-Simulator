package com.dss.backend.mapper;

import com.dss.backend.dto.NodeDTO;
import com.dss.backend.model.Node;
import org.mapstruct.Mapper;

/**
 * {@code NodeMapper} maps between domain {@link Node} objects and
 * their corresponding {@link NodeDTO} objects.
 *
 * <p>MapStruct will implement these methods automatically. Can
 * add additional mappings if fields differ by name or type.</p>
 *
 * <p><strong>Examples:</strong></p>
 * <ul>
 *   <li>Converting a persisted {@link Node} from the database into a
 *       {@link NodeDTO} for JSON output.</li>
 *   <li>Mapping a {@link NodeDTO} received in a REST request back
 *       into a {@link Node} entity for saving.</li>
 * </ul>
 */
@Mapper(componentModel = "spring")
public interface NodeMapper {

    /**
     * Converts a domain {@link Node} to a {@link NodeDTO}.
     *
     * @param node the source Node entity
     * @return the resulting NodeDTO
     */
    NodeDTO nodeToNodeDTO(Node node);

    /**
     * Converts a {@link NodeDTO} back into a domain {@link Node}.
     *
     * @param nodeDTO the source NodeDTO
     * @return a new Node entity
     */
    Node nodeDTOToNode(NodeDTO nodeDTO);
}