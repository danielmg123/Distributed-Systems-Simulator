package com.dss.backend.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * <p>
 * A custom JWT filter that intercepts every incoming HTTP request (once per request)
 * to check for a valid JSON Web Token (JWT) in the request's "Authorization" header.
 * </p>
 *
 * <p>
 * Steps performed:
 * <ol>
 *   <li>Extract the token from the <strong>Authorization</strong> header (Bearer scheme).</li>
 *   <li>Validate the token via {@link JwtTokenProvider}:
 *       <ul>
 *         <li>Check token integrity using the secret key.</li>
 *         <li>Check expiration timestamp to ensure the token has not expired.</li>
 *       </ul>
 *   </li>
 *   <li>If valid, the user’s username is retrieved, and {@link UserDetails} are loaded
 *       from the database (or in-memory) to build a Spring Security authentication object.</li>
 *   <li>Attach the authentication to the current {@link org.springframework.security.core.context.SecurityContext},
 *       making the user appear as authenticated for the rest of the request lifecycle.</li>
 *   <li>If invalid or missing, the filter does not authenticate the request and simply passes it down the chain.</li>
 * </ol>
 * </p>
 */
@Component
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    @Autowired
    private JwtTokenProvider tokenProvider;

    @Autowired
    private UserDetailsService userDetailsService;

    /**
     * <p>Performs the actual filtering logic on each incoming request.</p>
     *
     * <p>Algorithm:
     * <ul>
     *   <li>Grab the JWT from the "Authorization" header.</li>
     *   <li>Check if the token is non-empty and valid via {@code tokenProvider}.</li>
     *   <li>If valid, load the user by username and set authentication in
     *       {@code SecurityContextHolder}.</li>
     *   <li>Continue filter chain execution regardless of success/failure,
     *       so further filters and endpoints can proceed or handle exceptions.</li>
     * </ul>
     * </p>
     *
     * @param request  The HTTP request being processed.
     * @param response The HTTP response; can be used to set error status if needed.
     * @param filterChain The other filters in the chain; once finished, the request typically reaches a controller.
     * @throws ServletException in case of general servlet issues
     * @throws IOException in case of I/O errors
     */
    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain)
            throws ServletException, IOException {

        // Extract the token from Authorization header: "Bearer <token>"
        String token = getJwtFromRequest(request);

        // If the token is not empty and is valid, set up authentication
        if (StringUtils.hasText(token) && tokenProvider.validateToken(token)) {
            // Extract the username from the token
            String username = tokenProvider.getUsernameFromJWT(token);

            // Load the user details from DB or memory
            UserDetails userDetails = userDetailsService.loadUserByUsername(username);

            // Create an authentication object with the user details and set it into the security context
            UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(
                    userDetails, null, userDetails.getAuthorities());
            authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));

            SecurityContextHolder.getContext().setAuthentication(authentication);
        }

        // Continue with the filter chain (even if token was invalid, we do not block the request here)
        filterChain.doFilter(request, response);
    }

    /**
     * Helper method to parse the JWT from the HTTP "Authorization" header.
     *
     * @param request The current HTTP request.
     * @return the token string or {@code null} if none is found.
     */
    private String getJwtFromRequest(HttpServletRequest request) {
        String bearerToken = request.getHeader("Authorization");
        // Typically the Authorization header is: "Bearer <JWT>"
        if (StringUtils.hasText(bearerToken) && bearerToken.startsWith("Bearer ")) {
            // Strip off "Bearer " prefix to extract the raw token
            return bearerToken.substring(7);
        }
        return null;
    }
}