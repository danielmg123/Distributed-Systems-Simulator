package com.dss.backend.controller;

import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.model.Simulation;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.model.SimulationStatus;
import com.dss.backend.model.ConsensusAlgorithmType;
import com.dss.backend.model.TopologyType;
import com.dss.backend.service.SimulationService;
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
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
public class SimulationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SimulationService simulationService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @WithMockUser(username = "admin", roles = {"ADMIN"})
    public void createSimulation_withConfig() throws Exception {
        // Prepare a Simulation object with an embedded SimulationConfig.
        Simulation simulation = new Simulation();
        simulation.setId("sim1");
        simulation.setName("Test Simulation");
        simulation.setStatus(SimulationStatus.PAUSED);

        SimulationConfig config = new SimulationConfig();
        config.setAlgorithmType(ConsensusAlgorithmType.PAXOS);
        config.setNodeCount(5);
        config.setTopologyType(TopologyType.MESH);
        config.setFailurePercentage(10.0);
        config.setMetricsToCapture(java.util.Arrays.asList("latency", "throughput"));
        config.setTlsEnabled(true);

        simulation.setConfig(config);

        // When saveSimulation is called, return our prepared simulation.
        Mockito.when(simulationService.saveSimulation(Mockito.any(Simulation.class))).thenReturn(simulation);

        String simulationJson = objectMapper.writeValueAsString(simulation);

        mockMvc.perform(post("/api/simulations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(simulationJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is("sim1")))
                .andExpect(jsonPath("$.config.algorithmType", is("PAXOS")))
                .andExpect(jsonPath("$.config.nodeCount", is(5)))
                .andExpect(jsonPath("$.config.topologyType", is("MESH")))
                .andExpect(jsonPath("$.config.failurePercentage", is(10.0)))
                .andExpect(jsonPath("$.config.tlsEnabled", is(true)));
    }

    @Test
    @WithMockUser(username = "admin", roles = {"ADMIN"})
    public void runSimulation_shouldReturnOk() throws Exception {
        // For runSimulation, simulate a successful call.
        Mockito.doNothing().when(simulationService).runSimulation("sim1");

        mockMvc.perform(post("/api/simulations/sim1/run"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Simulation started with ID: sim1")));
    }

    @Test
    @WithMockUser(username = "admin", roles = {"ADMIN"})
    public void deleteSimulation_existingSimulation() throws Exception {
        // For an existing simulation, no exception is thrown.
        Mockito.doNothing().when(simulationService).deleteSimulation("sim1");

        mockMvc.perform(delete("/api/simulations/sim1"))
                .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(username = "admin", roles = {"ADMIN"})
    public void deleteSimulation_nonExistingSimulation() throws Exception {
        // For a non-existing simulation, simulate a ResourceNotFoundException.
        Mockito.doThrow(new ResourceNotFoundException("Simulation not found with id: sim2"))
                .when(simulationService).deleteSimulation("sim2");

        mockMvc.perform(delete("/api/simulations/sim2"))
                .andExpect(status().isNotFound());
    }
}
