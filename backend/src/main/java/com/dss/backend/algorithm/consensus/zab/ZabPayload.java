package com.dss.backend.algorithm.consensus.zab;

import com.dss.backend.engine.concurrent.MessageType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ZabPayload {
    // The type of Zab message: PROPOSAL, ACK, or COMMIT.
    private MessageType type;
    // The transaction identifier (zxid) for this proposal.
    private long zxid;
    // The value being proposed/committed.
    private Object proposedValue;
}
