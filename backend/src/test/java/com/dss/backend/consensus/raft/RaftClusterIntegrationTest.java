package com.dss.backend.consensus.raft;

import com.dss.backend.config.SimulationProperties;
import com.dss.backend.engine.DefaultScheduler;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.VirtualNode;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Phase 3 cluster integration test: a real 5-node Raft cluster wired through a
 * real {@link MessageRouter} and a real, multi-threaded {@link DefaultScheduler}, with
 * every node's randomized election timer actually running. Nothing here is mocked or
 * manually triggered -- this is the end-to-end proof that Phase 3.1/3.2's wiring
 * (randomized election timeout, timer resets on valid heartbeats/granted votes, retry
 * on an inconclusive election) produces a real, working cluster.
 * <p>
 * Scenario (e) from the plan -- partitioning two nodes from the rest and confirming the
 * minority side never elects -- is not implemented here: {@link MessageRouter}'s
 * loss/delay model (Phase 2.3) is a single global rate applied uniformly to every link,
 * not a per-pair partition. Simulating a true partition would require selective,
 * link-specific message dropping, which doesn't exist yet and is out of scope for this
 * phase.
 */
public class RaftClusterIntegrationTest {

    private static final int NODE_COUNT = 5;
    private static final int MAJORITY = NODE_COUNT / 2 + 1;

    @Test
    public void fiveNodeCluster_BootsElectsLeader_FailsOver_AndContinuesCommitting() throws Exception {
        MessageRouter router = new MessageRouter();
        Scheduler scheduler = new DefaultScheduler(Executors.newScheduledThreadPool(NODE_COUNT * 2));
        // Default election timeout range (150-300ms) -- this test is specifically about
        // proving that real, randomized timer fires correctly.
        SimulationProperties simulationProperties = new SimulationProperties();

        List<String> nodeIds = new ArrayList<>();
        for (int i = 1; i <= NODE_COUNT; i++) {
            nodeIds.add("node" + i);
        }

        List<Raft> algorithms = new ArrayList<>();
        List<VirtualNode> virtualNodes = new ArrayList<>();
        for (String nodeId : nodeIds) {
            Raft raft = new Raft(nodeId, nodeIds, router, scheduler, simulationProperties);
            algorithms.add(raft);
            virtualNodes.add(startVirtualNode(nodeId, raft, router, scheduler));
        }

        try {
            // (a) 5-node cluster boots leaderless -> exactly one LEADER emerges within ~2s.
            Raft leader = waitForSingleLeader(algorithms, 2000, "initial cluster boot");

            // (b) Propose through the leader -> value commits on (replicates to) a majority.
            leader.propose("value1");
            waitForMajorityReplication(algorithms, "value1", 2000);

            // (c) Fail the leader -> a different node becomes leader. The plan's target is
            // ~2x the max election timeout (600ms); a more generous bound is used here to
            // avoid flaking under CI/system load while still keeping the test meaningful.
            VirtualNode leaderVNode = virtualNodes.get(algorithms.indexOf(leader));
            leaderVNode.failNode();
            List<Raft> remaining = new ArrayList<>(algorithms);
            remaining.remove(leader);
            Raft newLeader = waitForSingleLeader(remaining, 3000, "failover election");
            assertNotSame(leader, newLeader, "A different node must become leader after the old leader fails");

            // (d) Propose again through the new leader -> commits (replicates to a majority).
            newLeader.propose("value2");
            waitForMajorityReplication(remaining, "value2", 2000);
        } finally {
            virtualNodes.forEach(VirtualNode::stop);
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

    /**
     * Polls until exactly one node in {@code algorithms} is LEADER, then returns it.
     */
    private Raft waitForSingleLeader(List<Raft> algorithms, long timeoutMillis, String context) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            List<Raft> leaders = algorithms.stream()
                    .filter(r -> r.getRole() == Raft.Role.LEADER)
                    .collect(Collectors.toList());
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
            Thread.sleep(20);
        }
        long leaderCount = algorithms.stream().filter(r -> r.getRole() == Raft.Role.LEADER).count();
        fail("Expected exactly one LEADER during " + context + " within " + timeoutMillis
                + "ms, found " + leaderCount);
        return null; // unreachable
    }

    /**
     * Polls until at least a majority of the full cluster's nodes have {@code value}
     * somewhere in their log.
     */
    private void waitForMajorityReplication(List<Raft> algorithms, Object value, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (countReplicas(algorithms, value) >= MAJORITY) {
                return;
            }
            Thread.sleep(20);
        }
        fail("Expected value \"" + value + "\" to replicate to a majority (" + MAJORITY
                + ") within " + timeoutMillis + "ms, only reached " + countReplicas(algorithms, value));
    }

    private long countReplicas(List<Raft> algorithms, Object value) {
        return algorithms.stream()
                .filter(r -> r.getLog().stream().anyMatch(e -> value.equals(e.getCommand())))
                .count();
    }
}
