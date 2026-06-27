package com.dss.backend.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The simulator is an unauthenticated demo tool, so every endpoint must be reachable
 * without credentials. This guards against accidentally reintroducing an auth wall.
 */
@SpringBootTest
@AutoConfigureMockMvc
public class SecurityConfigTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void allEndpointsArePublic() throws Exception {
        mockMvc.perform(get("/api/nodes")).andExpect(status().isOk());
        mockMvc.perform(get("/api/simulations")).andExpect(status().isOk());
        mockMvc.perform(get("/api/topologies")).andExpect(status().isOk());
    }
}
