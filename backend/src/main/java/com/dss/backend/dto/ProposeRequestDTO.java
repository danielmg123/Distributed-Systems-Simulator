package com.dss.backend.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Data Transfer Object for proposing a value to a running simulation's consensus algorithm.")
public class ProposeRequestDTO {

    @Schema(description = "The value to propose. Broadcast to every active node; each node's algorithm " +
            "decides whether to act on it (e.g. only the current Raft leader does).", example = "set x=1")
    private Object value;
}
