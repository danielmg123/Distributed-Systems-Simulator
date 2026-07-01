package com.dss.backend.config;

import org.springframework.beans.factory.annotation.Value;
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
     * Browser origin allowed to open the WebSocket, driven by the same {@code ALLOWED_ORIGIN}
     * environment variable as the REST CORS config (default: the local dev dashboard). Set
     * it to the real dashboard URL at deploy time to lock the handshake down to that domain.
     */
    @Value("${ALLOWED_ORIGIN:http://localhost:3000}")
    private String allowedOrigin;

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
        // The "/ws" endpoint is where clients initiate the WebSocket handshake. The
        // handshake origin is restricted to ALLOWED_ORIGIN (defaulting to the local dev
        // dashboard) rather than "*", so a deployment locks it to its own domain.
        registry.addEndpoint("/ws").setAllowedOriginPatterns(allowedOrigin).withSockJS();
    }
}
