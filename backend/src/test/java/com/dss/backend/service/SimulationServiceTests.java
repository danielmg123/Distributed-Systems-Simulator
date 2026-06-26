package com.dss.backend.service;

import com.dss.backend.config.SimulationProperties;
import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.model.Node;
import com.dss.backend.model.Simulation;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.model.SimulationStatus;
import com.dss.backend.model.TopologyType;
import com.dss.backend.repository.NodeRepository;
import com.dss.backend.repository.SimulationRepository;
import com.dss.backend.controller.SimulationWebSocketController;
import com.dss.backend.service.engine.SimulationOrchestrator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SimulationServiceTests {

    @Mock
    private SimulationRepository simulationRepository;

    @Mock
    private NodeRepository nodeRepository;

    @Mock
    private SimulationWebSocketController simulationWebSocketController;

    // Add a mock for SimulationProperties
    @Mock
    private SimulationProperties simulationProperties;

    // Also add a mock for Scheduler to avoid NPEs.
    @Mock
    private com.dss.backend.engine.Scheduler scheduler;

    @InjectMocks
    private SimulationService simulationService;

    private Simulation simulation;
    private SimulationConfig config;

    @BeforeEach
    public void setup() {
        simulation = new Simulation();
        simulation.setId("sim1");
        simulation.setName("Test Simulation");
        simulation.setStatus(SimulationStatus.PAUSED);

        config = new SimulationConfig();
        config.setTopologyType(TopologyType.MESH);
        simulation.setConfig(config);

        // Stub simulationProperties to avoid NPEs.
        when(simulationProperties.getWorkerThreadPoolSize()).thenReturn(10);
        when(simulationProperties.getHeartbeatIntervalMillis()).thenReturn(1000L);
        when(simulationProperties.getMultipaxosPrepareTimeoutMillis()).thenReturn(5000L);
    }

    @Test
    public void getAllSimulations_ReturnsAllSimulations() {
        List<Simulation> sims = Arrays.asList(simulation);
        when(simulationRepository.findAll()).thenReturn(sims);

        List<Simulation> result = simulationService.getAllSimulations();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("sim1", result.get(0).getId());
    }

    @Test
    public void saveSimulation_ValidObject_SavesInRepository() {
        when(simulationRepository.save(simulation)).thenReturn(simulation);

        Simulation saved = simulationService.saveSimulation(simulation);
        assertNotNull(saved);
        assertEquals("sim1", saved.getId());
        verify(simulationRepository, times(1)).save(simulation);
    }

    @Test
    public void runSimulation_ValidId_StartsOrchestrator() {
        when(simulationRepository.findById("sim1")).thenReturn(Optional.of(simulation));

        // Provide nodes with valid IDs
        Node node1 = new Node();
        node1.setId("node1");
        Node node2 = new Node();
        node2.setId("node2");
        when(nodeRepository.findAll()).thenReturn(Arrays.asList(node1, node2));

        simulationService.runSimulation("sim1");

        // Verify that the simulation status was updated to RUNNING.
        ArgumentCaptor<Simulation> simCaptor = ArgumentCaptor.forClass(Simulation.class);
        verify(simulationRepository).save(simCaptor.capture());
        Simulation updated = simCaptor.getValue();
        assertEquals(SimulationStatus.RUNNING, updated.getStatus());

        // Use reflection to verify that the private orchestrators map contains "sim1".
        try {
            Field field = SimulationService.class.getDeclaredField("orchestrators");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, SimulationOrchestrator> orchestrators = (Map<String, SimulationOrchestrator>) field.get(simulationService);
            assertTrue(orchestrators.containsKey("sim1"));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Reflection error: " + e.getMessage());
        }
    }

    @Test
    public void stopSimulation_ValidId_StopsAndRemovesOrchestrator() {
        try {
            Field field = SimulationService.class.getDeclaredField("orchestrators");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, SimulationOrchestrator> orchestrators = (Map<String, SimulationOrchestrator>) field.get(simulationService);

            SimulationOrchestrator dummyOrch = mock(SimulationOrchestrator.class);
            orchestrators.put("sim1", dummyOrch);
            when(simulationRepository.findById("sim1")).thenReturn(Optional.of(simulation));

            simulationService.stopSimulation("sim1");

            verify(dummyOrch, times(1)).stopSimulation("sim1");
            assertFalse(orchestrators.containsKey("sim1"));

            ArgumentCaptor<Simulation> simCaptor = ArgumentCaptor.forClass(Simulation.class);
            verify(simulationRepository).save(simCaptor.capture());
            Simulation updated = simCaptor.getValue();
            assertEquals(SimulationStatus.COMPLETED, updated.getStatus());
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Reflection error: " + e.getMessage());
        }
    }

    @Test
    public void failNode_ValidSimulationAndNode_CallsOrchestratorFailNode() {
        try {
            Field field = SimulationService.class.getDeclaredField("orchestrators");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, SimulationOrchestrator> orchestrators = (Map<String, SimulationOrchestrator>) field.get(simulationService);

            SimulationOrchestrator dummyOrch = mock(SimulationOrchestrator.class);
            orchestrators.put("sim1", dummyOrch);

            simulationService.failNode("sim1", "node1");

            verify(dummyOrch, times(1)).failNode("sim1", "node1");
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Reflection error: " + e.getMessage());
        }
    }

    @Test
    public void getTopologyMapping_RunningSimulation_ReturnsOrchestratorsMapping() {
        try {
            Field field = SimulationService.class.getDeclaredField("orchestrators");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, SimulationOrchestrator> orchestrators = (Map<String, SimulationOrchestrator>) field.get(simulationService);

            SimulationOrchestrator dummyOrch = mock(SimulationOrchestrator.class);
            Map<String, List<String>> expectedMapping = Map.of("node1", Arrays.asList("node2", "node3"));
            when(dummyOrch.getTopologyMapping()).thenReturn(expectedMapping);
            orchestrators.put("sim1", dummyOrch);

            Map<String, List<String>> result = simulationService.getTopologyMapping("sim1");

            assertEquals(expectedMapping, result);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Reflection error: " + e.getMessage());
        }
    }

    @Test
    public void getTopologyMapping_NoRunningSimulation_ReturnsEmptyMap() {
        Map<String, List<String>> result = simulationService.getTopologyMapping("nonexistent-sim");

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void updateNetworkConditions_RunningSimulation_DelegatesToOrchestrator() {
        try {
            Field field = SimulationService.class.getDeclaredField("orchestrators");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, SimulationOrchestrator> orchestrators = (Map<String, SimulationOrchestrator>) field.get(simulationService);

            SimulationOrchestrator dummyOrch = mock(SimulationOrchestrator.class);
            orchestrators.put("sim1", dummyOrch);

            simulationService.updateNetworkConditions("sim1", 0.25, 50L, 300L);

            verify(dummyOrch, times(1)).setNetworkConditions(0.25, 50L, 300L);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Reflection error: " + e.getMessage());
        }
    }

    @Test
    public void updateNetworkConditions_NoRunningSimulation_IsNoOp() {
        // No orchestrator registered for "nonexistent-sim" -- must not throw.
        assertDoesNotThrow(() -> simulationService.updateNetworkConditions("nonexistent-sim", 0.5, 10L, 100L));
    }
}