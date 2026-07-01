package com.dss.backend.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter;
import org.springframework.security.web.header.writers.XXssProtectionHeaderWriter;

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
}
