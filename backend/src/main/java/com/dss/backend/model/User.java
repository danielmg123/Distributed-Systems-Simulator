package com.dss.backend.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Represents an application user for authentication and authorization.
 *
 * <p>Fields:</p>
 * <ul>
 *   <li>{@code username} - The unique login name.</li>
 *   <li>{@code password} - A BCrypt-hashed password (never stored in plaintext).</li>
 *   <li>{@code role} - Typically "USER" or "ADMIN". In Spring Security,
 *       we might see "ROLE_USER" or "ROLE_ADMIN".</li>
 * </ul>
 */
@Document(collection = "users")
@Data
public class User {

    @Id
    private String id;

    private String username;
    private String password; // Stored as a BCrypt hash
    private String role;     // "ADMIN" or "USER"
}