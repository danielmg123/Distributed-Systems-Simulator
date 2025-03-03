package com.dss.backend.config;

import com.dss.backend.engine.concurrent.MessageRouter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
public class AppConfig {
    @Bean
    public ScheduledExecutorService scheduledExecutorService() {
        // You can adjust the number of threads as needed:
        return Executors.newScheduledThreadPool(5);
    }

    @Bean
    public MessageRouter messageRouter() {
        return new MessageRouter();
    }
}