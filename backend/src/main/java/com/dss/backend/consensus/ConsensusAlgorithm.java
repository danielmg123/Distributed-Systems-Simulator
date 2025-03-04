package com.dss.backend.consensus;

import com.dss.backend.messaging.SimulationMessage;

public interface ConsensusAlgorithm {
    void propose(Object value);
    boolean accept(Object proposal);
    void commit(Object value);
    void handleMessage(SimulationMessage msg);
}
