package com.dss.backend.security;

import com.dss.backend.model.User;
import com.dss.backend.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

/**
 * <p>
 * A {@code CustomUserDetailsService} that loads user information from MongoDB and
 * produces a Spring Security {@link UserDetails} instance for authentication.
 * </p>
 *
 * <p>
 * This service is used to tie application's user records to Spring Security's
 * authentication mechanism. By implementing {@link UserDetailsService}, Spring Security
 * will invoke {@link #loadUserByUsername(String)} to locate and authenticate users
 * based on their username.
 * </p>
 *
 * <p><strong>Role Handling:</strong><br/>
 * The user's role is stored as a simple string field (e.g., "ADMIN"). Here, Spring Security
 * automatically prefixes it with "ROLE_" to match standard role naming conventions
 * (e.g., "ROLE_ADMIN"). The returned {@link UserDetails} object is then used
 * to apply authorization rules in {@code SecurityConfig} or
 * in any method-level security annotations.
 * </p>
 *
 * @author Daniel Morales
 */
@Service
public class CustomUserDetailsService implements UserDetailsService {

    @Autowired
    private UserRepository userRepository;

    /**
     * Loads a user's details from the MongoDB {@code users} collection by their username.
     * <ul>
     *   <li>If a matching user is found, returns a fully populated {@link UserDetails}
     *       with credentials and granted roles.</li>
     *   <li>If no user is found, throws a {@link UsernameNotFoundException}.</li>
     * </ul>
     *
     * @param username The unique username to be used for searching the user record.
     * @return A Spring Security {@link UserDetails} object containing the user's credentials and roles.
     * @throws UsernameNotFoundException if no user with the specified {@code username} is found.
     */
    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        // Attempt to find the user by username.
        User user = userRepository.findByUsername(username);
        if (user == null) {
            // If not found, let Spring Security know the user does not exist.
            throw new UsernameNotFoundException("User not found: " + username);
        }
        // Build a UserDetails object that Spring Security can work with.
        // The 'roles()' method automatically adds "ROLE_" prefix to the stored role.
        return org.springframework.security.core.userdetails.User.builder()
                .username(user.getUsername())
                .password(user.getPassword())  // The hashed password
                .roles(user.getRole())        // e.g., "ADMIN" becomes "ROLE_ADMIN" internally
                .build();
    }
}