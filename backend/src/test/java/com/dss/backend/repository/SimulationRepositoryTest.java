package com.dss.backend.repository;

import com.dss.backend.model.Simulation;
import com.dss.backend.model.SimulationStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DataMongoTest
public class SimulationRepositoryTest {

    @Autowired
    private SimulationRepository simulationRepository;

    @Test
    public void testFindByStatus() {
        // Clear repository
        simulationRepository.deleteAll();

        // Insert simulations with different statuses.
        Simulation sim1 = new Simulation();
        sim1.setName("Sim1");
        sim1.setStatus(SimulationStatus.RUNNING);
        simulationRepository.save(sim1);

        Simulation sim2 = new Simulation();
        sim2.setName("Sim2");
        sim2.setStatus(SimulationStatus.PAUSED);
        simulationRepository.save(sim2);

        Simulation sim3 = new Simulation();
        sim3.setName("Sim3");
        sim3.setStatus(SimulationStatus.RUNNING);
        simulationRepository.save(sim3);

        // Query by RUNNING status.
        List<Simulation> runningSims = simulationRepository.findByStatus(SimulationStatus.RUNNING);
        assertNotNull(runningSims);
        assertEquals(2, runningSims.size());
    }

    @Test
    public void testFindByName() {
        // Clear repository
        simulationRepository.deleteAll();

        // Insert simulations with unique names.
        Simulation simAlpha = new Simulation();
        simAlpha.setName("Alpha");
        simAlpha.setStatus(SimulationStatus.PAUSED);
        simulationRepository.save(simAlpha);

        Simulation simBeta = new Simulation();
        simBeta.setName("Beta");
        simBeta.setStatus(SimulationStatus.RUNNING);
        simulationRepository.save(simBeta);

        // Query by name "Alpha"
        Simulation foundAlpha = simulationRepository.findByName("Alpha");
        assertNotNull(foundAlpha);
        assertEquals("Alpha", foundAlpha.getName());

        // Query by name "Beta"
        Simulation foundBeta = simulationRepository.findByName("Beta");
        assertNotNull(foundBeta);
        assertEquals("Beta", foundBeta.getName());
    }
}
