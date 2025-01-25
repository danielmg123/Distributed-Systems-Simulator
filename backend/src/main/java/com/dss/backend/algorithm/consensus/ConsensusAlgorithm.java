package com.dss.backend.algorithm.consensus;

public interface ConsensusAlgorithm {
    void propose(Object value);
    boolean accept(Object proposal);
    void commit(Object value);
}
