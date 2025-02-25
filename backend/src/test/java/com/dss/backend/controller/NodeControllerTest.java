package com.dss.backend.controller;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.service.NodeService;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
public class NodeControllerTest {
/*
    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private NodeService nodeService;

    @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    public void createNode_AdminAccess() throws Exception {
        String nodeJson = "{\"address\": \"123.456.789.000\", \"status\": \"ACTIVE\"}";
        mockMvc.perform(post("/api/nodes")
               .contentType("application/json")
               .content(nodeJson))
               .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(username = "user", roles = "USER")
    public void deleteNode_UserAccessDenied() throws Exception {
        mockMvc.perform(delete("/api/nodes/1"))
               .andExpect(status().isForbidden());
    }
    
    @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    public void getAllNodes_AdminAccess() throws Exception {
        mockMvc.perform(get("/api/nodes"))
               .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(username = "user", roles = "USER")
    public void getNodeById_UserAccessDenied() throws Exception {
        mockMvc.perform(get("/api/nodes/1"))
               .andExpect(status().isForbidden());
    }

        @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    public void deleteNode_AdminAccess_EntityExists() throws Exception {
        String nodeId = "1";
        Mockito.doNothing().when(nodeService).deleteNode(nodeId);

        mockMvc.perform(delete("/api/nodes/" + nodeId))
               .andExpect(status().isOk());

        Mockito.verify(nodeService, Mockito.times(1)).deleteNode(nodeId);
    }

    @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    public void deleteNode_AdminAccess_EntityNotExists() throws Exception {
        String nodeId = "non-existent-id";
        Mockito.doThrow(new ResourceNotFoundException("Node not found")).when(nodeService).deleteNode(nodeId);

        mockMvc.perform(delete("/api/nodes/" + nodeId))
               .andExpect(status().isNotFound());
    }
 */
}