package com.dss.backend.config;

import com.dss.backend.engine.DefaultScheduler;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.messaging.MessageRouter;
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
    public Scheduler scheduler() {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(simulationProperties.getSchedulerThreadPoolSize());
        return new DefaultScheduler(service);
    }

    @Bean
    public MessageRouter messageRouter() {
        return new MessageRouter();
    }
}