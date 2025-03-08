package com.dss.backend.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
// Set the property so that security is disabled.
@TestPropertySource(properties = {"app.security.disable=true"})
public class SecurityConfigDisabledTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void whenSecurityDisabled_allEndpointsPermitAll() throws Exception {
        // With security disabled, all endpoints should be accessible without authentication.
        mockMvc.perform(get("/api/nodes"))
                .andExpect(status().isOk());
        mockMvc.perform(get("/api/simulations"))
                .andExpect(status().isOk());
        mockMvc.perform(get("/api/topologies"))
                .andExpect(status().isOk());
    }
}