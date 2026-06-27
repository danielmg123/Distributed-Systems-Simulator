package com.dss.backend.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Application-wide configuration.
 *
 * <p>Enables binding of {@link SimulationProperties} from {@code application.properties}.
 * Per-simulation infrastructure ({@code Scheduler}, {@code MessageRouter}) is intentionally
 * <em>not</em> defined here as shared singletons: each simulation run constructs its own in
 * {@link com.dss.backend.service.SimulationService#runSimulation(String)} so that stopping
 * one simulation (which shuts down its scheduler) cannot affect any other run.</p>
 */
@Configuration
@EnableConfigurationProperties(SimulationProperties.class)
public class AppConfig {
}