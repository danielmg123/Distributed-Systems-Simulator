package com.dss.backend.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter;
import org.springframework.security.web.header.writers.XXssProtectionHeaderWriter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.List;

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

    /**
     * The single browser origin allowed to call the API cross-origin. Defaults to the
     * local dev dashboard; set the {@code ALLOWED_ORIGIN} environment variable to the
     * real dashboard URL at deploy time to lock CORS down to that domain. In the normal
     * bundled setup the dashboard is served same-origin (Vite proxy in dev, nginx proxy
     * in prod), so this only matters for direct cross-origin access to the backend.
     */
    @Value("${ALLOWED_ORIGIN:http://localhost:3000}")
    private String allowedOrigin;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.csrf(csrf -> csrf.disable())
                .cors(Customizer.withDefaults())
                .authorizeHttpRequests(auth -> auth.anyRequest().permitAll())
                // Baseline security headers on every response. Set explicitly (rather than
                // relying on framework defaults) so the policy is guaranteed and readable.
                .headers(headers -> headers
                        // X-Content-Type-Options: nosniff -- don't let browsers MIME-sniff.
                        .contentTypeOptions(Customizer.withDefaults())
                        // X-Frame-Options: DENY -- this API/dashboard is never framed.
                        .frameOptions(frame -> frame.deny())
                        // X-XSS-Protection: 0 -- the legacy auditor is disabled per modern
                        // guidance (it can introduce its own vulnerabilities); rely on CSP/nosniff.
                        .xssProtection(xss -> xss.headerValue(XXssProtectionHeaderWriter.HeaderValue.DISABLED))
                        // Referrer-Policy: strict-origin-when-cross-origin -- send only the
                        // origin (not the full path) when navigating cross-origin.
                        .referrerPolicy(referrer -> referrer.policy(
                                ReferrerPolicyHeaderWriter.ReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN)));
        return http.build();
    }

    /**
     * Restricts cross-origin API access to {@link #allowedOrigin}. No credentials/cookies
     * are used (the tool is unauthenticated), so allowCredentials stays false.
     */
    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration config = new CorsConfiguration();
        config.setAllowedOrigins(List.of(allowedOrigin));
        config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "OPTIONS"));
        config.setAllowedHeaders(List.of("*"));
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/api/**", config);
        return source;
    }
}
