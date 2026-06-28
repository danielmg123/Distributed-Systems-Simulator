package com.dss.backend.service.engine;

import com.dss.backend.engine.Scheduler;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.VirtualNode;
import com.dss.backend.model.EventType;
import com.dss.backend.model.NodeStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers the failed-leader feedback in {@link SimulationOrchestrator#propose}: a proposal
 * to a leader-based protocol with no active leader should log an explanatory event, while
 * a healthy leader-based protocol (or a leaderless one like Paxos) should not.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SimulationOrchestratorTest {

    private static final String NO_LEADER = "no node is currently the leader";

    @Mock private MessageRouter router;
    @Mock private Scheduler scheduler;
    @Mock private NodeInitializationService nodeInitializationService;
    @Mock private MetricsUpdateService metricsUpdateService;
    @Mock private EventLoggerService eventLoggerService;

    private SimulationOrchestrator orchestratorWith(Map<String, VirtualNode> nodes) {
        when(nodeInitializationService.initializeNodes(any(), any(), any())).thenReturn(nodes);
        SimulationOrchestrator orchestrator = new SimulationOrchestrator(
                router, scheduler, nodeInitializationService, metricsUpdateService, eventLoggerService);
        // topologyType null -> nodeMap is set to the mocked node map, no TopologyPlacer call
        orchestrator.initializeSimulationNodes(Collections.emptyList(), null, null);
        return orchestrator;
    }

    private VirtualNode node(NodeStatus status, String roleLabel) {
        VirtualNode vNode = mock(VirtualNode.class);
        when(vNode.getNodeStatus()).thenReturn(status);
        when(vNode.getRoleLabel()).thenReturn(roleLabel);
        return vNode;
    }

    @Test
    public void propose_leaderBasedWithNoActiveLeader_logsNoLeaderEvent() {
        Map<String, VirtualNode> nodes = new LinkedHashMap<>();
        nodes.put("node1", node(NodeStatus.FAILED, "LEADER"));   // the leader has failed
        nodes.put("node2", node(NodeStatus.ACTIVE, "FOLLOWER"));
        nodes.put("node3", node(NodeStatus.ACTIVE, "FOLLOWER"));

        orchestratorWith(nodes).propose("sim1", "x=1");

        verify(eventLoggerService).logEvent(eq("sim1"), contains(NO_LEADER), eq(EventType.SIMULATION_EVENT));
    }

    @Test
    public void propose_leaderBasedWithActiveLeader_doesNotLogNoLeaderEvent() {
        Map<String, VirtualNode> nodes = new LinkedHashMap<>();
        nodes.put("node1", node(NodeStatus.ACTIVE, "LEADER"));
        nodes.put("node2", node(NodeStatus.ACTIVE, "FOLLOWER"));

        orchestratorWith(nodes).propose("sim1", "x=1");

        verify(eventLoggerService, never()).logEvent(anyString(), contains(NO_LEADER), any());
    }

    @Test
    public void propose_leaderlessProtocol_doesNotLogNoLeaderEvent() {
        Map<String, VirtualNode> nodes = new LinkedHashMap<>();
        nodes.put("node1", node(NodeStatus.ACTIVE, "ACCEPTOR"));
        nodes.put("node2", node(NodeStatus.ACTIVE, "ACCEPTOR"));

        orchestratorWith(nodes).propose("sim1", "x=1");

        verify(eventLoggerService, never()).logEvent(anyString(), contains(NO_LEADER), any());
    }
}
