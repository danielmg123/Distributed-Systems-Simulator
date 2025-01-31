package com.dss.backend.algorithm.consensus;

import com.dss.backend.engine.concurrent.SimulationMessage;

public interface ConsensusAlgorithm {
    void propose(Object value);
    boolean accept(Object proposal);
    void commit(Object value);
    void handleMessage(SimulationMessage msg);
}
