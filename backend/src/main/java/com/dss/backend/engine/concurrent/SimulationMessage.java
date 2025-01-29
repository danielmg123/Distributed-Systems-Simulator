package com.dss.backend.engine.concurrent;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SimulationMessage {
    private String sourceNodeId;
    private String targetNodeId;
    private MessageType type;
    private Object payload;
}