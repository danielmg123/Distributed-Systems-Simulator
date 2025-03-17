package com.dss.backend.repository;

import com.dss.backend.model.User;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * {@code UserRepository} manages {@link User} entities in MongoDB, typically for
 * authentication and authorization.
 *
 * <p>Besides the basic CRUD provided by {@link MongoRepository}, it includes:
 * <ul>
 *   <li>{@link #findByUsername(String)} – Retrieves the user record associated
 *       with the supplied username.</li>
 * </ul></p>
 *
 * <p><strong>Security Note:</strong>
 * {@code password} fields are stored using BCrypt hashing.</p>
 */
public interface UserRepository extends MongoRepository<User, String> {

    /**
     * Finds a user by their unique username.
     *
     * @param username the username credential to look for
     * @return the corresponding {@link User}, or null if none found
     */
    User findByUsername(String username);
}