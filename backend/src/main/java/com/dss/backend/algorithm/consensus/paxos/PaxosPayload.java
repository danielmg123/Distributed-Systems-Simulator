package com.dss.backend.algorithm.consensus.paxos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaxosPayload {
    private int proposalNumber;
    private int acceptedId;       // Used when responding with existing accepted proposal
    private Object acceptedValue; // The previously accepted value (if any)
    private Object proposedValue; // The new value being proposed
    // Additional fields if needed (e.g. node IDs, round IDs, etc....)
}
