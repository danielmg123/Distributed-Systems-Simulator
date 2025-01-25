package com.dss.backend.algorithm.consensus.zab;

import org.springframework.stereotype.Component;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;

@Component
public class Zab implements ConsensusAlgorithm {

    @Override
    public void propose(Object value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'propose'");
    }

    @Override
    public boolean accept(Object proposal) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'accept'");
    }

    @Override
    public void commit(Object value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'commit'");
    }
    
}
