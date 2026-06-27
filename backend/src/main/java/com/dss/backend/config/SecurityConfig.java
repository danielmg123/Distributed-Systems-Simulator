package com.dss.backend.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Security configuration for the simulator.
 *
 * <p>The simulator is an <strong>unauthenticated local demo / educational tool</strong>:
 * there is no login, no user accounts, and no token handling. Every endpoint is open.
 * CSRF protection is disabled because the API is stateless and is only driven by the
 * bundled dashboard. Do not expose this service on an untrusted network.</p>
 */
@Configuration
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.csrf(csrf -> csrf.disable())
                .authorizeHttpRequests(auth -> auth.anyRequest().permitAll());
        return http.build();
    }
}
