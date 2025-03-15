package com.dss.backend.config;

import com.dss.backend.security.JwtAuthenticationFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Configures Spring Security for the application.
 * <p>
 * If security is disabled (e.g., for local testing), most checks will be bypassed.
 * In production, set <code>app.security.disable=false</code> so that endpoints
 * are protected by HTTP Basic or JWT token-based authentication.
 */
@Configuration
public class SecurityConfig {

    /**
     * Flag indicating if security should be disabled.
     * For example, 'true' in test profiles or local development.
     */
    @Value("${app.security.disable:false}")
    private boolean securityDisabled;

    @Autowired
    private JwtAuthenticationFilter jwtAuthenticationFilter;

    /**
     * Configures the {@link SecurityFilterChain} with basic constraints.
     * <ul>
     *   <li>If security is disabled, all requests are permitted.</li>
     *   <li>If enabled, /api/nodes/** requires ROLE_ADMIN,
     *       and other endpoints require at least authentication or are public.</li>
     * </ul>
     *
     * @param http the {@link HttpSecurity} to configure
     * @return the built {@link SecurityFilterChain}
     * @throws Exception if any configuration error occurs
     */
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        if (securityDisabled) {
            // -------------------------------------------------------------
            // Testing / Local Dev Mode: Allow everything without auth
            // -------------------------------------------------------------
            http.csrf(csrf -> csrf.disable())
                    .authorizeHttpRequests(auth -> auth.anyRequest().permitAll());
        } else {
            // -------------------------------------------------------------
            // Production Mode: Secure endpoints
            // -------------------------------------------------------------
            http.csrf(csrf -> csrf.disable())
                    .authorizeHttpRequests(auth -> auth
                            // Admin role required for node mgmt
                            .requestMatchers("/api/nodes/**").hasRole("ADMIN")
                            // Let some endpoints be public or require normal user auth
                            .requestMatchers("/api/algorithms/**", "/api/simulations/**", "/api/topologies/**").permitAll()
                            .anyRequest().authenticated()
                    )
                    // Example: using Basic auth
                    .httpBasic(Customizer.withDefaults());

            // If using JWT, place the JwtAuthenticationFilter in the chain:
            // .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);
        }
        return http.build();
    }
}