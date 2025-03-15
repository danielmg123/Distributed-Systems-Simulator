package com.dss.backend.config;

import com.dss.backend.engine.DefaultScheduler;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.messaging.MessageRouter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Application-wide configuration class that defines beans for
 * scheduling tasks and message routing.
 *
 * <p>By centralizing these beans here, we ensure that different parts
 * of the simulator can share a common Scheduler and MessageRouter,
 * avoiding duplication or inconsistent configurations.</p>
 */
@Configuration
@EnableConfigurationProperties(SimulationProperties.class)
public class AppConfig {

    private final SimulationProperties simulationProperties;

    public AppConfig(SimulationProperties simulationProperties) {
        this.simulationProperties = simulationProperties;
    }

    /**
     * Creates a shared {@link Scheduler} bean that uses a thread pool of size
     * specified in {@code simulation.schedulerThreadPoolSize}.
     * <p>
     * This scheduler is used by many simulation components (heartbeats, consensus timeouts, etc.).
     *
     * @return a {@link Scheduler} instance backed by a ScheduledExecutorService
     */
    @Bean
    public Scheduler scheduler() {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(
                simulationProperties.getSchedulerThreadPoolSize()
        );
        return new DefaultScheduler(service);
    }

    /**
     * Creates a singleton {@link MessageRouter} bean that routes messages
     * (e.g., heartbeats, Paxos messages) between {@code VirtualNode} instances.
     * <p>
     * Having a shared router allows consistent logging and hooking into message flow.
     *
     * @return a new {@link MessageRouter} instance
     */
    @Bean
    public MessageRouter messageRouter() {
        return new MessageRouter();
    }
}