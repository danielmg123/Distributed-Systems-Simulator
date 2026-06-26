package com.dss.backend.controller;

import com.dss.backend.dto.SimulationDTO;
import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.mapper.EventMapper;
import com.dss.backend.mapper.SimulationMapper;
import com.dss.backend.model.Simulation;
import com.dss.backend.model.SimulationStatus;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
public class SimulationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SimulationService simulationService;

    @MockBean
    private SimulationMapper simulationMapper;

    @MockBean
    private EventMapper eventMapper;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @WithMockUser(username = "user", roles = {"USER"})
    public void createSimulation_ValidRequest_SavesAndReturnsSimulation() throws Exception {
        Simulation simulation = new Simulation();
        simulation.setId("sim1");
        simulation.setName("Test Simulation");
        simulation.setStatus(SimulationStatus.PAUSED);

        SimulationDTO simulationDTO = new SimulationDTO();
        simulationDTO.setId("sim1");
        simulationDTO.setName("Test Simulation");

        Mockito.when(simulationMapper.simulationDTOToSimulation(Mockito.any(SimulationDTO.class)))
                .thenReturn(simulation);
        Mockito.when(simulationService.saveSimulation(simulation)).thenReturn(simulation);
        Mockito.when(simulationMapper.simulationToSimulationDTO(simulation)).thenReturn(simulationDTO);

        String simulationJson = objectMapper.writeValueAsString(simulationDTO);

        mockMvc.perform(post("/api/simulations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(simulationJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is("sim1")))
                .andExpect(jsonPath("$.name", is("Test Simulation")));
    }

    @Test
    @WithMockUser(username = "user", roles = {"USER"})
    public void getSimulationById_ValidId_ReturnsSimulation() throws Exception {
        Simulation simulation = new Simulation();
        simulation.setId("sim1");
        simulation.setName("Test Simulation");

        SimulationDTO simulationDTO = new SimulationDTO();
        simulationDTO.setId("sim1");
        simulationDTO.setName("Test Simulation");

        Mockito.when(simulationService.getSimulationByIdOrThrow("sim1")).thenReturn(simulation);
        Mockito.when(simulationMapper.simulationToSimulationDTO(simulation)).thenReturn(simulationDTO);

        mockMvc.perform(get("/api/simulations/sim1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is("sim1")))
                .andExpect(jsonPath("$.name", is("Test Simulation")));
    }

    @Test
    @WithMockUser(username = "user", roles = {"USER"})
    public void runSimulation_ValidId_StartsSimulation() throws Exception {
        Mockito.doNothing().when(simulationService).runSimulation("sim1");

        mockMvc.perform(post("/api/simulations/sim1/run"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Simulation started with ID: sim1")));
    }

    @Test
    @WithMockUser(username = "user", roles = {"USER"})
    public void failNode_ValidIds_FailsNode() throws Exception {
        Mockito.doNothing().when(simulationService).failNode("sim1", "node1");

        mockMvc.perform(post("/api/simulations/sim1/failNode/node1"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Node node1 failed in simulation sim1")));
    }

    @Test
    @WithMockUser(username = "user", roles = {"USER"})
    public void getSimulationTopology_ValidId_ReturnsNeighborMapping() throws Exception {
        Mockito.when(simulationService.getTopologyMapping("sim1"))
                .thenReturn(java.util.Map.of("node1", java.util.List.of("node2", "node3")));

        mockMvc.perform(get("/api/simulations/sim1/topology"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.node1[0]", is("node2")))
                .andExpect(jsonPath("$.node1[1]", is("node3")));
    }
}