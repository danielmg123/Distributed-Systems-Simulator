package com.dss.backend.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * Configures STOMP-based WebSocket messaging for the simulator.
 * <p>
 * Clients connect to the <code>/ws</code> endpoint and subscribe
 * to topics (like <code>/topic/simulation/{id}/events</code>) to receive updates.
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    /**
     * Configure the in-memory message broker with a topic prefix.
     *
     * @param config the registry used to set broker properties
     */
    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        // Enable a simple in-memory broker with the prefix "/topic".
        // The simulator uses topic destinations for broadcasting events and metrics.
        config.enableSimpleBroker("/topic");
        // Prefix for messages bound for @MessageMapping-annotated methods
        config.setApplicationDestinationPrefixes("/app");
    }

    /**
     * Registers the STOMP endpoint that clients (e.g., front-end) will use
     * to establish the initial WebSocket connection.
     *
     * <p>By adding SockJS fallback, older browsers or environments
     * lacking native WebSocket support can still connect.</p>
     *
     * @param registry the registry for mapping endpoints
     */
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // The "/ws" endpoint is where clients initiate the WebSocket handshake.
        // setAllowedOriginPatterns("*") for dev/demo. Lock this down in production.
        registry.addEndpoint("/ws").setAllowedOriginPatterns("*").withSockJS();
    }
}
