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
import com.dss.backend.model.Simulation;
import com.dss.backend.service.SimulationService;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
public class SimulationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SimulationService simulationService;

    @Test
    @WithMockUser(username = "user", roles = "USER")
    public void getAllSimulations_UserAccess() throws Exception {
        mockMvc.perform(get("/api/simulations"))
               .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    public void getSimulationById_AdminAccess() throws Exception {
        Simulation mockSimulation = new Simulation();
        mockSimulation.setId("1");
        mockSimulation.setName("Test Simulation");
        Mockito.when(simulationService.getSimulationByIdOrThrow("1")).thenReturn(mockSimulation);

        mockMvc.perform(get("/api/simulations/1"))
               .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    public void deleteSimulation_AdminAccess_EntityExists() throws Exception {
        String simulationId = "1";
        Mockito.doNothing().when(simulationService).deleteSimulation(simulationId);

        mockMvc.perform(delete("/api/simulations/" + simulationId))
               .andExpect(status().isOk());

        Mockito.verify(simulationService, Mockito.times(1)).deleteSimulation(simulationId);
    }

    @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    public void deleteSimulation_AdminAccess_EntityNotExists() throws Exception {
        String simulationId = "non-existent-id";
        Mockito.doThrow(new ResourceNotFoundException("Simulation not found")).when(simulationService).deleteSimulation(simulationId);

        mockMvc.perform(delete("/api/simulations/" + simulationId))
               .andExpect(status().isNotFound());
    }
}
