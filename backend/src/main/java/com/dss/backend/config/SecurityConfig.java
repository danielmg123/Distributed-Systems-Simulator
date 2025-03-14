package com.dss.backend.config;

import com.dss.backend.security.JwtAuthenticationFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
public class SecurityConfig {

    // If true, security is turned off (for testing)
    @Value("${app.security.disable:false}")
    private boolean securityDisabled;

    @Autowired
    private JwtAuthenticationFilter jwtAuthenticationFilter;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        if (securityDisabled) {
            // Disable CSRF and permit all requests.
            http.csrf(csrf -> csrf.disable())
                    .authorizeHttpRequests(auth -> auth.anyRequest().permitAll());
        } else {
            // Production mode: secure endpoints appropriately.
            http.csrf(csrf -> csrf.disable())
                    .authorizeHttpRequests(auth -> auth
                            // Only allow users with ADMIN role to access /api/nodes
                            .requestMatchers("/api/nodes/**").hasRole("ADMIN")
                            // Let these endpoints be public:
                            .requestMatchers("/api/algorithms/**", "/api/simulations/**", "/api/topologies/**").permitAll()
                            // All other requests require authentication.
                            .anyRequest().authenticated()
                    )
                    // Use basic auth (or JWT)
                    .httpBasic(Customizer.withDefaults());
            // If using JWT, add filter:
            // .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);
        }
        return http.build();
    }
}