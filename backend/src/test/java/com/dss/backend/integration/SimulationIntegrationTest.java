package com.dss.backend.integration;

import com.dss.backend.config.SimulationProperties;
import com.dss.backend.dto.NodeDTO;
import com.dss.backend.dto.SimulationConfigDTO;
import com.dss.backend.dto.SimulationDTO;
import com.dss.backend.metrics.MetricsSnapshot;
import com.dss.backend.model.ConsensusAlgorithmType;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import com.dss.backend.model.Simulation;
import com.dss.backend.model.SimulationConfig;
import com.dss.backend.model.SimulationStatus;
import com.dss.backend.model.TopologyType;
import com.dss.backend.repository.NodeRepository;
import com.dss.backend.repository.SimulationRepository;
import com.dss.backend.service.SimulationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest  // Loads the full application context (including MongoDB)
@AutoConfigureMockMvc
public class SimulationIntegrationTest {

    @Autowired
    private SimulationService simulationService;

    @Autowired
    private SimulationRepository simulationRepository;

    @Autowired
    private NodeRepository nodeRepository;

    @Autowired
    private SimulationProperties simulationProperties;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MockMvc mockMvc;

    // Prepare some nodes for the simulation.
    @BeforeEach
    public void setUp() {
        // Clear existing simulation and node data.
        simulationRepository.deleteAll();
        nodeRepository.deleteAll();

        // Create 3 active nodes.
        Node node1 = new Node();
        node1.setId("node1");
        node1.setAddress("192.168.1.1");
        node1.setStatus(NodeStatus.ACTIVE);

        Node node2 = new Node();
        node2.setId("node2");
        node2.setAddress("192.168.1.2");
        node2.setStatus(NodeStatus.ACTIVE);

        Node node3 = new Node();
        node3.setId("node3");
        node3.setAddress("192.168.1.3");
        node3.setStatus(NodeStatus.ACTIVE);

        nodeRepository.saveAll(Arrays.asList(node1, node2, node3));
    }

    // --- Integration test using the service layer ---
    @Test
    public void startAndStopSimulation_IntegrationFlow() throws Exception {
        // Create a Simulation with configuration.
        Simulation simulation = new Simulation();
        simulation.setId(UUID.randomUUID().toString());
        simulation.setName("Integration Simulation");
        simulation.setStatus(SimulationStatus.PAUSED);
        SimulationConfig config = new SimulationConfig();
        config.setAlgorithmType(ConsensusAlgorithmType.PAXOS);
        config.setNodeCount(3);
        config.setTopologyType(TopologyType.MESH);
        config.setFailurePercentage(0.0); // No automatic failure for this test
        config.setMetricsToCapture(Arrays.asList("latency", "throughput"));
        config.setTlsEnabled(false);
        simulation.setConfig(config);
        simulationRepository.save(simulation);

        // Run the simulation.
        simulationService.runSimulation(simulation.getId());

        // Let the simulation run briefly.
        Thread.sleep(5000);

        // Stop the simulation.
        simulationService.stopSimulation(simulation.getId());

        // Verify that the simulation status is COMPLETED.
        Simulation updated = simulationRepository.findById(simulation.getId())
                .orElseThrow(() -> new RuntimeException("Simulation not found"));
        assertEquals(SimulationStatus.COMPLETED, updated.getStatus());

        // Verify that metrics are available.
        MetricsSnapshot snapshot = simulationService.getSimulationMetrics(simulation.getId());
        assertNotNull(snapshot, "Metrics snapshot should not be null");
    }

    // --- End-to-end REST endpoints test ---
    @Test
    public void restEndpoints_FullFlow() throws Exception {
        // 1. POST a new Simulation.
        SimulationDTO simDto = new SimulationDTO();
        simDto.setName("Full Flow Simulation");
        SimulationConfigDTO configDto = new SimulationConfigDTO();
        configDto.setAlgorithmType(ConsensusAlgorithmType.PAXOS);
        configDto.setNodeCount(3);
        configDto.setTopologyType(TopologyType.MESH);
        configDto.setFailurePercentage(10.0);  // Simulate 10% chance of node failure
        configDto.setMetricsToCapture(Arrays.asList("latency", "throughput"));
        configDto.setTlsEnabled(false);
        simDto.setConfig(configDto);

        String simJson = objectMapper.writeValueAsString(simDto);

        String response = mockMvc.perform(post("/api/simulations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(simJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").exists())
                .andReturn().getResponse().getContentAsString();

        SimulationDTO createdSim = objectMapper.readValue(response, SimulationDTO.class);
        String simId = createdSim.getId();

        // 2. Run the simulation.
        mockMvc.perform(post("/api/simulations/" + simId + "/run"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Simulation started with ID: " + simId)));

        // 3. Fail a node.
        mockMvc.perform(post("/api/simulations/" + simId + "/failNode/node1"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Node node1 failed in simulation " + simId)));

        // 4. Fetch simulation metrics.
        mockMvc.perform(get("/api/simulations/" + simId + "/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalMessages").exists());

        // 5. Stop the simulation.
        mockMvc.perform(post("/api/simulations/" + simId + "/stop"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Simulation stopped for ID: " + simId)));
    }

    /**
     * Regression test for the per-simulation scheduler fix (B2).
     * <p>
     * The scheduler used to be a shared singleton bean that {@code stopSimulation}
     * shut down globally, so the <em>second</em> simulation in a process silently lost
     * its Raft election timers and never elected a leader. This test runs a Raft
     * simulation, stops it, then runs a second one and asserts the second simulation
     * still elects a leader -- which only holds if each run gets its own scheduler.
     */
    @Test
    public void runStopRun_secondRaftSimulationStillElectsLeader() throws Exception {
        // First simulation: run, confirm it elects a leader, then stop it (shutting
        // down its scheduler -- which previously poisoned the shared singleton).
        String firstId = createAndSaveRaftSimulation("First Raft Simulation");
        simulationService.runSimulation(firstId);
        assertTrue(waitForLeader(firstId, 5000),
                "First simulation should elect a Raft leader");
        simulationService.stopSimulation(firstId);

        // Second simulation: with a per-run scheduler this must still elect a leader.
        String secondId = createAndSaveRaftSimulation("Second Raft Simulation");
        simulationService.runSimulation(secondId);
        try {
            assertTrue(waitForLeader(secondId, 5000),
                    "Second simulation must still elect a leader after the first was stopped "
                            + "(regression: a shared scheduler would be shut down by the first stop)");
        } finally {
            simulationService.stopSimulation(secondId);
        }
    }

    @Test
    public void metrics_recordMessagesProposalsAndCommits() throws Exception {
        String id = createAndSaveRaftSimulation("Metrics Simulation");
        simulationService.runSimulation(id);
        try {
            assertTrue(waitForLeader(id, 5000), "a leader must be elected before proposing");
            simulationService.propose(id, "x=1");

            // The shared collector accumulates across simulations, so assert >= 1 rather
            // than == 1. The point is that these counters move at all (they were dead).
            MetricsSnapshot snap = null;
            boolean recorded = false;
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                snap = simulationService.getSimulationMetrics(id);
                if (snap.getTotalProposals() >= 1 && snap.getTotalCommits() >= 1) {
                    recorded = true;
                    break;
                }
                Thread.sleep(50);
            }
            assertTrue(recorded, "expected a proposal and a commit to be recorded, got " + snap);
            assertTrue(snap.getTotalMessages() > 0, "expected delivered messages to be counted, got " + snap);
        } finally {
            simulationService.stopSimulation(id);
        }
    }

    @Test
    public void nodeStatuses_exposeCommittedValueAndDetailAfterCommit() throws Exception {
        String id = createAndSaveRaftSimulation("Committed Value Simulation");
        simulationService.runSimulation(id);
        try {
            assertTrue(waitForLeader(id, 5000), "a leader must be elected before proposing");
            simulationService.propose(id, "x=1");

            // The leader commits the single entry deterministically; assert its node
            // status carries the committed value, and that Raft nodes report a detail
            // summary (term/committed count).
            boolean found = false;
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                List<NodeDTO> statuses = simulationService.getNodeStatuses(id);
                boolean committedShown = statuses.stream().anyMatch(n -> "x=1".equals(n.getCommittedValue()));
                boolean detailShown = statuses.stream().allMatch(n -> n.getDetail() != null && n.getDetail().startsWith("term"));
                if (committedShown && detailShown) {
                    found = true;
                    break;
                }
                Thread.sleep(50);
            }
            assertTrue(found, "expected the committed value and Raft state detail to surface in node statuses");
        } finally {
            simulationService.stopSimulation(id);
        }
    }

    private String createAndSaveRaftSimulation(String name) {
        Simulation simulation = new Simulation();
        simulation.setId(UUID.randomUUID().toString());
        simulation.setName(name);
        simulation.setStatus(SimulationStatus.PAUSED);
        SimulationConfig config = new SimulationConfig();
        config.setAlgorithmType(ConsensusAlgorithmType.RAFT);
        config.setNodeCount(3);
        config.setTopologyType(TopologyType.MESH);
        config.setFailurePercentage(0.0);
        simulation.setConfig(config);
        simulationRepository.save(simulation);
        return simulation.getId();
    }

    /** Polls the live node statuses until some node reports the Raft LEADER role, or the deadline passes. */
    private boolean waitForLeader(String simulationId, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            List<NodeDTO> statuses = simulationService.getNodeStatuses(simulationId);
            if (statuses.stream().anyMatch(n -> "LEADER".equals(n.getRoleLabel()))) {
                return true;
            }
            Thread.sleep(50);
        }
        return false;
    }
}