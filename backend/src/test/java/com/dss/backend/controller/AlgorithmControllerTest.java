package com.dss.backend.controller;

import com.dss.backend.dto.AlgorithmDTO;
import com.dss.backend.model.ConsensusAlgorithmType;
import com.dss.backend.service.AlgorithmService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import java.util.Arrays;
import java.util.List;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
public class AlgorithmControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AlgorithmService algorithmService;

    @Test
    public void getAvailableAlgorithms_ReturnsAlgorithmList() throws Exception {

        List<AlgorithmDTO> algorithms = Arrays.asList(
                new AlgorithmDTO("Paxos", ConsensusAlgorithmType.PAXOS),
                new AlgorithmDTO("Raft", ConsensusAlgorithmType.RAFT)
        );

        Mockito.when(algorithmService.getAvailableAlgorithms()).thenReturn(algorithms);

        mockMvc.perform(get("/api/algorithms")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", not(empty())))
                .andExpect(jsonPath("$[0].name", is("Paxos")))
                .andExpect(jsonPath("$[0].type", is("PAXOS")));
    }
}