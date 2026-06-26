package com.dss.backend.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Data Transfer Object for live-updating a running simulation's simulated network conditions.")
public class NetworkConditionsDTO {

    @Schema(description = "Probability (0.0-1.0) that any given message is randomly dropped in transit, " +
            "independent of node failures. 0 disables random loss.", example = "0.1")
    private double messageLossRate;

    @Schema(description = "Minimum simulated message delivery delay, in milliseconds. Has no effect unless " +
            "maxDelayMs is also set to a positive value.", example = "50")
    private long minDelayMs;

    @Schema(description = "Maximum simulated message delivery delay, in milliseconds. 0 disables delay " +
            "simulation entirely, delivering messages synchronously.", example = "200")
    private long maxDelayMs;
}
