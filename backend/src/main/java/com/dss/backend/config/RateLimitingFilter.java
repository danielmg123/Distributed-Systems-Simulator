package com.dss.backend.config;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * In-memory, per-IP rate limiting for the mutating API endpoints, to blunt abuse on a
 * public deployment without pulling in Redis or any other external service. Each client
 * IP gets a token bucket of {@value #CAPACITY} requests per minute; when it's empty the
 * request is rejected with 429 Too Many Requests.
 *
 * <p>Only the mutation endpoints are limited -- create a simulation, propose a value, and
 * fail or recover a node. The read-only endpoints (node statuses, topology, metrics) are
 * left alone: the dashboard polls them on a timer and they don't change anything.</p>
 *
 * <p>Buckets live in a plain map keyed by client IP, which grows with the number of
 * distinct IPs seen. For a demo tool in a short-lived process that's acceptable. Behind
 * the bundled nginx proxy the real client IP arrives in {@code X-Forwarded-For}, so we
 * prefer that (falling back to the socket address) -- otherwise every user would share
 * the proxy's single IP and one client's traffic would throttle everyone.</p>
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RateLimitingFilter extends OncePerRequestFilter {

    private static final int CAPACITY = 20;
    private static final Duration WINDOW = Duration.ofMinutes(1);

    // POST create-simulation, propose, failNode/{id}, recoverNode/{id}. The run and stop
    // endpoints are deliberately excluded -- they're bounded by the simulation lifecycle.
    private static final Pattern RATE_LIMITED_PATHS = Pattern.compile(
            "^/api/simulations(/[^/]+/(propose|failNode/[^/]+|recoverNode/[^/]+))?$");

    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        if (!isRateLimited(request)) {
            filterChain.doFilter(request, response);
            return;
        }
        Bucket bucket = buckets.computeIfAbsent(clientIp(request), ip -> newBucket());
        if (bucket.tryConsume(1)) {
            filterChain.doFilter(request, response);
        } else {
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value()); // 429
            response.setContentType("application/json");
            response.getWriter().write(
                    "{\"status\":429,\"error\":\"Too Many Requests\","
                            + "\"message\":\"Rate limit exceeded. Please slow down and try again shortly.\"}");
        }
    }

    private boolean isRateLimited(HttpServletRequest request) {
        return "POST".equalsIgnoreCase(request.getMethod())
                && RATE_LIMITED_PATHS.matcher(request.getRequestURI()).matches();
    }

    private Bucket newBucket() {
        Bandwidth limit = Bandwidth.builder()
                .capacity(CAPACITY)
                .refillGreedy(CAPACITY, WINDOW)
                .build();
        return Bucket.builder().addLimit(limit).build();
    }

    /** Prefer the proxy-forwarded client IP; fall back to the socket address. */
    private String clientIp(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            // X-Forwarded-For can be a comma-separated chain; the first entry is the client.
            return forwarded.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
