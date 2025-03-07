package com.dss.backend.controller;

import com.dss.backend.dto.NodeDTO;
import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.mapper.NodeMapper;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import com.dss.backend.service.NodeService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
public class NodeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private NodeService nodeService;

    @MockBean
    private NodeMapper nodeMapper;

    @Test
    @WithMockUser(username = "admin", roles = {"ADMIN"})
    public void getAllNodes_ReturnsListOfNodes() throws Exception {
        List<Node> nodes = Arrays.asList(
                createNode("id1", "address1", NodeStatus.ACTIVE),
                createNode("id2", "address2", NodeStatus.ACTIVE)
        );
        Mockito.when(nodeService.getAllNodes()).thenReturn(nodes);
        Mockito.when(nodeMapper.nodeToNodeDTO(Mockito.any(Node.class)))
                .thenAnswer(invocation -> {
                    Node n = invocation.getArgument(0);
                    NodeDTO dto = new NodeDTO();
                    dto.setId(n.getId());
                    dto.setAddress(n.getAddress());
                    dto.setStatus(n.getStatus().name());
                    return dto;
                });

        mockMvc.perform(get("/api/nodes"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0].id", is("id1")))
                .andExpect(jsonPath("$[0].address", is("address1")));
    }

    @Test
    @WithMockUser(username = "admin", roles = {"ADMIN"})
    public void getNodeById_ValidId_ReturnsNode() throws Exception {
        Node node = createNode("id1", "address1", NodeStatus.ACTIVE);
        NodeDTO nodeDTO = new NodeDTO();
        nodeDTO.setId("id1");
        nodeDTO.setAddress("address1");
        nodeDTO.setStatus("ACTIVE");

        Mockito.when(nodeService.getNodeByIdOrThrow("id1")).thenReturn(node);
        Mockito.when(nodeMapper.nodeToNodeDTO(node)).thenReturn(nodeDTO);

        mockMvc.perform(get("/api/nodes/id1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is("id1")))
                .andExpect(jsonPath("$.address", is("address1")));
    }

    @Test
    @WithMockUser(username = "admin", roles = {"ADMIN"})
    public void getNodeById_InvalidId_ThrowsResourceNotFound() throws Exception {
        Mockito.when(nodeService.getNodeByIdOrThrow("invalid"))
                .thenThrow(new ResourceNotFoundException("Node not found"));

        mockMvc.perform(get("/api/nodes/invalid"))
                .andExpect(status().isNotFound());
    }

    @Test
    @WithMockUser(username = "admin", roles = {"ADMIN"})
    public void createNode_ValidDTO_SavesNode() throws Exception {
        // JSON input (without id since it is generated)
        String nodeJson = "{\"address\":\"address1\",\"status\":\"ACTIVE\"}";
        Node node = createNode("id1", "address1", NodeStatus.ACTIVE);
        NodeDTO createdDTO = new NodeDTO();
        createdDTO.setId("id1");
        createdDTO.setAddress("address1");
        createdDTO.setStatus("ACTIVE");

        Mockito.when(nodeMapper.nodeDTOToNode(Mockito.any(NodeDTO.class))).thenReturn(node);
        Mockito.when(nodeService.saveNode(node)).thenReturn(node);
        Mockito.when(nodeMapper.nodeToNodeDTO(node)).thenReturn(createdDTO);

        mockMvc.perform(post("/api/nodes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(nodeJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is("id1")))
                .andExpect(jsonPath("$.address", is("address1")));
    }

    @Test
    @WithMockUser(username = "admin", roles = {"ADMIN"})
    public void deleteNode_ExistingId_DeletesNode() throws Exception {
        Mockito.doNothing().when(nodeService).deleteNode("id1");

        mockMvc.perform(delete("/api/nodes/id1"))
                .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(username = "admin", roles = {"ADMIN"})
    public void deleteNode_NonExistentId_ThrowsResourceNotFound() throws Exception {
        Mockito.doThrow(new ResourceNotFoundException("Node not found")).when(nodeService).deleteNode("invalid");

        mockMvc.perform(delete("/api/nodes/invalid"))
                .andExpect(status().isNotFound());
    }

    private Node createNode(String id, String address, NodeStatus status) {
        Node n = new Node();
        n.setId(id);
        n.setAddress(address);
        n.setStatus(status);
        return n;
    }
}