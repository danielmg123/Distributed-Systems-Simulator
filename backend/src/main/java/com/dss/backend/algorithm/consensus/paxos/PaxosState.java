package com.dss.backend.algorithm.consensus.paxos;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class PaxosState {
    // Unique identifier of this node, often used in proposal # generation
    private final String nodeId;

    private int promisedId = -1;       // Highest proposal ID we have promised not to accept below
    private int acceptedId = -1;       // Highest proposal ID we have accepted
    private Object acceptedValue = null; // Value associated with the highest accepted proposal

    // Additional fields for the logic if needed
    // e.g. a local proposal counter for generating unique IDs
    // or a variable to store "chosen" value once consensus is reached.
}
