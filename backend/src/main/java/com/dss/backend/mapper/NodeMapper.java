package com.dss.backend.mapper;

import com.dss.backend.dto.NodeDTO;
import com.dss.backend.model.Node;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface NodeMapper {

    NodeDTO nodeToNodeDTO(Node node);

    Node nodeDTOToNode(NodeDTO nodeDTO);
}