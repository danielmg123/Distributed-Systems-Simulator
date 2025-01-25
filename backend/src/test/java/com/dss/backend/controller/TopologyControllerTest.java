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
import com.dss.backend.model.NetworkTopology;
import com.dss.backend.model.TopologyType;
import com.dss.backend.service.NetworkTopologyService;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
public class TopologyControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private NetworkTopologyService networkTopologyService;

    @Test
    @WithMockUser(username = "user", roles = "USER")
    public void getAllTopologies_UserAccess() throws Exception {
        mockMvc.perform(get("/api/topologies"))
               .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    public void getTopologyById_AdminAccess() throws Exception {
        NetworkTopology mockTopology = new NetworkTopology();
        mockTopology.setId("1");
        mockTopology.setType(TopologyType.MESH);
        Mockito.when(networkTopologyService.getTopologyByIdOrThrow("1")).thenReturn(mockTopology);

        mockMvc.perform(get("/api/topologies/1"))
               .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    public void deleteTopology_AdminAccess_EntityExists() throws Exception {
        String topologyId = "1";
        Mockito.doNothing().when(networkTopologyService).deleteTopology(topologyId);

        mockMvc.perform(delete("/api/topologies/" + topologyId))
               .andExpect(status().isOk());

        Mockito.verify(networkTopologyService, Mockito.times(1)).deleteTopology(topologyId);
    }

    @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    public void deleteTopology_AdminAccess_EntityNotExists() throws Exception {
        String topologyId = "non-existent-id";
        Mockito.doThrow(new ResourceNotFoundException("Topology not found")).when(networkTopologyService).deleteTopology(topologyId);

        mockMvc.perform(delete("/api/topologies/" + topologyId))
               .andExpect(status().isNotFound());
    }
}
