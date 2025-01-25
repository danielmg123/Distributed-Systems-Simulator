package com.dss.backend.service;

import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.model.NetworkTopology;
import com.dss.backend.repository.NetworkTopologyRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class NetworkTopologyService {

    @Autowired
    private NetworkTopologyRepository networkTopologyRepository;

    public List<NetworkTopology> getAllTopologies() {
        return networkTopologyRepository.findAll();
    }

    public NetworkTopology getTopologyByIdOrThrow(String id) {
        return networkTopologyRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("NetworkTopology not found with id: " + id));
    }

    public NetworkTopology saveTopology(NetworkTopology topology) {
        return networkTopologyRepository.save(topology);
    }

    public void deleteTopology(String id) {
        networkTopologyRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException("NetworkTopology not found with id: " + id));
        networkTopologyRepository.deleteById(id);
    }
}
