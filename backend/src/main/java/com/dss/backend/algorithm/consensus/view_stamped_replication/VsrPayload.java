package com.dss.backend.algorithm.consensus.view_stamped_replication;

import com.dss.backend.engine.concurrent.MessageType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VsrPayload {
    // The type of VSR message (PREPARE, PREPARE_RESPONSE, or COMMIT)
    private MessageType type;
    // The current view number
    private int view;
    // The operation sequence number (unique per proposal)
    private int opNum;
    // The proposed value (if any)
    private Object proposedValue;
}
