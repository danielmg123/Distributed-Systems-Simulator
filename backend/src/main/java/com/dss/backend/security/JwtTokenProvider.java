package com.dss.backend.security;

import io.jsonwebtoken.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * <p>
 * A utility class for creating, parsing, and validating JSON Web Tokens (JWTs).
 * </p>
 *
 * <p>
 * Responsibilities:
 * <ul>
 *   <li><strong>Generate Tokens:</strong>
 *       Use the configured secret key and expiration time to produce a signed JWT
 *       containing the username as the subject.</li>
 *   <li><strong>Extract Username:</strong>
 *       After validating the token signature, parse out the subject (username) from its claims.</li>
 *   <li><strong>Validation:</strong>
 *       Ensure the token is well-formed, has not expired, and the signature matches
 *       the secret key. If any check fails, a {@link JwtException} is raised or we return {@code false}.</li>
 * </ul>
 * </p>
 *
 * <p>
 * These tokens are typically included in the "Authorization" header with a "Bearer " prefix.
 * The matching filter, {@link JwtAuthenticationFilter}, relies on these methods to authenticate requests.
 * </p>
 */
@Component
public class JwtTokenProvider {

    /**
     * The secret key used to sign and verify JWT tokens.
     * Should be kept private and stored securely (e.g., environment variable).
     */
    @Value("${app.jwt.secret}")
    private String jwtSecret;

    /**
     * The expiration time (in milliseconds) after which the token is considered invalid.
     * For example, 86400000 = 24 hours.
     */
    @Value("${app.jwt.expirationInMs:86400000}")
    private int jwtExpirationInMs;

    /**
     * Creates a signed JWT string containing the username as the subject.
     * The token will expire after {@code jwtExpirationInMs} from the current time.
     *
     * @param username the username to embed within the JWT subject.
     * @return the signed JWT string.
     */
    public String generateToken(String username) {
        Date now = new Date();
        Date expiryDate = new Date(now.getTime() + jwtExpirationInMs);

        // Build the token with the username (subject), issue time, expiration time, and sign with the secret key.
        return Jwts.builder()
                .setSubject(username)
                .setIssuedAt(now)
                .setExpiration(expiryDate)
                .signWith(SignatureAlgorithm.HS512, jwtSecret)
                .compact();
    }

    /**
     * Extracts the username (subject) from a valid token's claims.
     *
     * @param token the raw JWT string.
     * @return the username embedded in the token subject.
     * @throws JwtException if the token is invalid or can't be parsed.
     */
    public String getUsernameFromJWT(String token) {
        Claims claims = Jwts.parser()
                .setSigningKey(jwtSecret)
                .parseClaimsJws(token)
                .getBody();
        return claims.getSubject();
    }

    /**
     * Validates the token by parsing and ensuring it has not expired
     * and the signature matches the known secret.
     *
     * @param authToken the raw JWT token string.
     * @return {@code true} if the token is valid, otherwise {@code false}.
     */
    public boolean validateToken(String authToken) {
        try {
            // If parsing or signature check fail, parseClaimsJws() throws an exception.
            Jwts.parser().setSigningKey(jwtSecret).parseClaimsJws(authToken);
            return true;
        } catch (JwtException ex) {
            // We could log the reason for failure (expired, malformed, etc.) here if needed.
        }
        return false;
    }
}