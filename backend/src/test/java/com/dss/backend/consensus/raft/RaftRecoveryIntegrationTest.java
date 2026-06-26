package com.dss.backend.consensus.raft;

import com.dss.backend.engine.DefaultScheduler;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.VirtualNode;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Phase 2.5 (node recovery): real {@link Raft} instances wired
 * through a real {@link MessageRouter} and {@link VirtualNode}s, proving that once a
 * failed leader recovers, the *existing* {@code nextIndexMap}/backoff logic in
 * {@link Raft#handleAppendEntriesResponse} -- already exercised by ordinary log
 * replication -- is what catches it back up. No new catch-up machinery is added for
 * this; the point of this test is to prove the existing mechanism actually fires.
 * <p>
 * Phase 3's randomized election timeout isn't wired up yet, so elections here are
 * triggered by calling the (already public) {@link Raft#triggerElection()} directly
 * rather than waiting for a timer -- everything downstream of that call (REQUEST_VOTE
 * broadcast, vote granting, majority count, AppendEntries replication, conflict
 * backoff) runs for real over the router, nothing is mocked.
 */
public class RaftRecoveryIntegrationTest {

    @Test
    public void leaderFailureAndRecovery_NewLeaderTakesOver_RecoveredNodeCatchesUp() throws Exception {
        List<String> nodeIds = Arrays.asList("node1", "node2", "node3");
        MessageRouter router = new MessageRouter();
        Scheduler scheduler = new DefaultScheduler(Executors.newSingleThreadScheduledExecutor());

        Raft node1 = new Raft("node1", nodeIds, router);
        Raft node2 = new Raft("node2", nodeIds, router);
        Raft node3 = new Raft("node3", nodeIds, router);

        VirtualNode vNode1 = startVirtualNode("node1", node1, router, scheduler);
        VirtualNode vNode2 = startVirtualNode("node2", node2, router, scheduler);
        VirtualNode vNode3 = startVirtualNode("node3", node3, router, scheduler);

        try {
            // node1 becomes the initial leader (standing in for Phase 3's election timer).
            node1.triggerElection();
            waitForRole(node1, Raft.Role.LEADER, "node1 should become the initial leader");

            node1.propose("value1");
            waitForLogSize(node1, 1, "node1");
            waitForLogSize(node2, 1, "node2");
            waitForLogSize(node3, 1, "node3");

            // Simulate the leader crashing. Per Phase 2.1/2.2, this actually stops node1's
            // own processing and heartbeat, and the router drops messages to/from it --
            // it's deaf and mute, not just flagged.
            vNode1.failNode();
            assertEquals(NodeStatus.FAILED, vNode1.getNodeStatus());

            // Manually trigger node2's election (standing in for Phase 3's timeout firing
            // once node1's heartbeats stop arriving).
            node2.triggerElection();
            waitForRole(node2, Raft.Role.LEADER, "node2 should win the election once node1 is unreachable");

            node2.propose("value2");
            waitForLogSize(node2, 2, "node2");
            waitForLogSize(node3, 2, "node3");

            // Recover the old leader. It still thinks it's a stale LEADER from term 1
            // until it next hears from node2 with a higher term.
            vNode1.recoverNode();
            assertEquals(NodeStatus.ACTIVE, vNode1.getNodeStatus());

            // There's no periodic leader heartbeat broadcast in this implementation, so a
            // further propose is what actually drives the AppendEntries that catches the
            // recovered node back up.
            node2.propose("value3");

            waitForLogSize(node1, 3, "node1 (recovered)");
            waitForLogSize(node2, 3, "node2");
            waitForLogSize(node3, 3, "node3");

            assertEquals(getCommands(node2), getCommands(node1),
                    "Recovered node1's log should eventually match the current leader's log");
            assertEquals(getCommands(node2), getCommands(node3),
                    "node3's log should match the current leader's log");
            assertEquals(Raft.Role.FOLLOWER, getRole(node1),
                    "node1 should have stepped down to FOLLOWER once it saw node2's higher term");
        } finally {
            vNode1.stop();
            vNode2.stop();
            vNode3.stop();
            scheduler.shutdown();
        }
    }

    private VirtualNode startVirtualNode(String nodeId, Raft algorithm, MessageRouter router, Scheduler scheduler) {
        Node node = new Node();
        node.setId(nodeId);
        node.setStatus(NodeStatus.ACTIVE);
        VirtualNode vNode = new VirtualNode(node, algorithm, router, scheduler);
        vNode.start();
        router.registerNode(nodeId, vNode);
        return vNode;
    }

    private void waitForRole(Raft raft, Raft.Role expected, String message) throws Exception {
        for (int i = 0; i < 100; i++) {
            if (getRole(raft) == expected) {
                return;
            }
            Thread.sleep(20);
        }
        fail(message + " (still " + getRole(raft) + " after timeout)");
    }

    private void waitForLogSize(Raft raft, int expectedSize, String label) throws Exception {
        for (int i = 0; i < 100; i++) {
            if (getLog(raft).size() >= expectedSize) {
                return;
            }
            Thread.sleep(20);
        }
        fail(label + "'s log never reached size " + expectedSize + " (was " + getLog(raft).size() + ")");
    }

    private Raft.Role getRole(Raft raft) throws Exception {
        java.lang.reflect.Field f = Raft.class.getDeclaredField("role");
        f.setAccessible(true);
        return (Raft.Role) f.get(raft);
    }

    @SuppressWarnings("unchecked")
    private List<LogEntry> getLog(Raft raft) throws Exception {
        java.lang.reflect.Field f = Raft.class.getDeclaredField("log");
        f.setAccessible(true);
        return (List<LogEntry>) f.get(raft);
    }

    private List<Object> getCommands(Raft raft) throws Exception {
        List<Object> commands = new ArrayList<>();
        for (LogEntry entry : getLog(raft)) {
            commands.add(entry.getCommand());
        }
        return commands;
    }
}
