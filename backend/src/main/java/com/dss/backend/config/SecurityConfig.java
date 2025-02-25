package com.dss.backend.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Value("${app.security.disable:false}")
    private boolean disableSecurity;

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        if (disableSecurity) {
            http
                    .csrf(csrf -> csrf.disable())
                    .authorizeHttpRequests(authz -> authz.anyRequest().permitAll())
                    .httpBasic(httpBasic -> httpBasic.disable());
        } else {
            http
                    .csrf(csrf -> csrf.disable())
                    .authorizeHttpRequests(authz -> authz
                            .requestMatchers("/api/nodes/**").hasRole("ADMIN")
                            .requestMatchers("/api/simulations/**", "/api/topologies/**").hasAnyRole("USER", "ADMIN")
                            .requestMatchers("/", "/api/public/**").permitAll()
                            .anyRequest().authenticated()
                    )
                    .httpBasic(httpBasic -> {});
        }
        return http.build();
    }
}
