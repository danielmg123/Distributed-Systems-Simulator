package com.dss.backend.repository;

import com.dss.backend.model.Simulation;
import com.dss.backend.model.SimulationStatus;
import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface SimulationRepository extends MongoRepository<Simulation, String> {
    List<Simulation> findByStatus(SimulationStatus status);

    Simulation findByName(String name);
}
