package com.dss.backend.config;

import com.dss.backend.engine.concurrent.MessageRouter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
@EnableConfigurationProperties(SimulationProperties.class)
public class AppConfig {

    private final SimulationProperties simulationProperties;

    public AppConfig(SimulationProperties simulationProperties) {
        this.simulationProperties = simulationProperties;
    }

    @Bean
    public ScheduledExecutorService scheduledExecutorService() {
        return Executors.newScheduledThreadPool(simulationProperties.getSchedulerThreadPoolSize());
    }

    @Bean
    public MessageRouter messageRouter() {
        return new MessageRouter();
    }
}