package com.dss.backend.controller;

import com.dss.backend.model.NetworkTopology;
import com.dss.backend.model.TopologyType;
import com.dss.backend.service.NetworkTopologyService;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
public class TopologyControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private NetworkTopologyService networkTopologyService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @WithMockUser(username = "user", roles = {"USER"})
    public void getAllTopologies_ReturnsList() throws Exception {
        List<NetworkTopology> topologies = Arrays.asList(new NetworkTopology(), new NetworkTopology());
        Mockito.when(networkTopologyService.getAllTopologies()).thenReturn(topologies);

        mockMvc.perform(get("/api/topologies"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)));
    }

    @Test
    @WithMockUser(username = "user", roles = {"USER"})
    public void createTopology_ValidRequest_ReturnsCreatedTopology() throws Exception {
        NetworkTopology topology = new NetworkTopology();
        topology.setId("top1");
        topology.setType(TopologyType.MESH);

        Mockito.when(networkTopologyService.saveTopology(Mockito.any(NetworkTopology.class))).thenReturn(topology);

        String topologyJson = objectMapper.writeValueAsString(topology);

        mockMvc.perform(post("/api/topologies")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(topologyJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is("top1")))
                .andExpect(jsonPath("$.type", is("MESH")));
    }

    @Test
    @WithMockUser(username = "user", roles = {"USER"})
    public void deleteTopology_ValidId_DeletesTopology() throws Exception {
        Mockito.doNothing().when(networkTopologyService).deleteTopology("top1");

        mockMvc.perform(delete("/api/topologies/top1"))
                .andExpect(status().isOk());
    }
}