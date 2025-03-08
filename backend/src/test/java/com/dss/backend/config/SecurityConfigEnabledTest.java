package com.dss.backend.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
// Set the property so that security is enabled.
@TestPropertySource(properties = {"app.security.disable=false"})
public class SecurityConfigEnabledTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void whenSecurityEnabled_noAuthentication_thenUnauthorized() throws Exception {
        // Without any authentication, endpoints should require auth.
        // For example, /api/nodes should not be accessible.
        mockMvc.perform(get("/api/nodes"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @WithMockUser(roles = "USER")
    public void whenSecurityEnabled_userRoleAccessRestricted() throws Exception {
        // The /api/nodes endpoints require an ADMIN role.
        mockMvc.perform(get("/api/nodes"))
                .andExpect(status().isForbidden());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    public void whenSecurityEnabled_adminRoleAccessGranted() throws Exception {
        // With ADMIN role, /api/nodes should be accessible.
        mockMvc.perform(get("/api/nodes"))
                .andExpect(status().isOk());
    }
}