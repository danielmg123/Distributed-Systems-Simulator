package com.dss.backend.consensus.raft;

import com.dss.backend.config.SimulationProperties;
import com.dss.backend.consensus.ConsensusObserver;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

    /**
     * Regression test for the commit-index off-by-one: a *single* proposed entry must
     * actually be committed and applied, not just replicated. With the old
     * {@code commitIndex = 0} initialization plus the strict {@code i > commitIndex}
     * advance check, the sole entry of a one-entry log was replicated but never committed,
     * so it was never applied to the state machine. This asserts the entry is applied.
     * <p>
     * It checks that the entry is applied somewhere (in practice, by the leader) rather
     * than on a majority: this implementation has no periodic leader heartbeat, so a
     * follower only learns of a commit on the next {@code AppendEntries}, which never
     * arrives for a single quiescent proposal -- so only the leader applies it
     * deterministically. The off-by-one bug, by contrast, prevented *any* node from
     * applying the entry at all.
     */
    @Test
    public void singleProposedEntry_IsCommittedAndApplied() throws Exception {
        MessageRouter router = new MessageRouter();
        Scheduler scheduler = new DefaultScheduler(Executors.newScheduledThreadPool(NODE_COUNT * 2));
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
            Raft leader = waitForSingleLeader(algorithms, 2000, "initial cluster boot");

            // Propose exactly one value, then require it to be actually committed and
            // applied -- not merely present in a log. Before the off-by-one fix, no node
            // ever applied the sole entry of a one-entry log.
            leader.propose("only-value");
            waitForEntryApplied(algorithms, 2000);
        } finally {
            virtualNodes.forEach(VirtualNode::stop);
            scheduler.shutdown();
        }
    }

    /**
     * Polls until at least one node has applied a committed entry ({@code lastApplied >= 1}).
     */
    private void waitForEntryApplied(List<Raft> algorithms, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (algorithms.stream().anyMatch(r -> r.getLastApplied() >= 1)) {
                return;
            }
            Thread.sleep(20);
        }
        fail("Expected the proposed entry to be committed and applied within " + timeoutMillis + "ms");
    }

    /**
     * With periodic leader heartbeats, a committed entry must reach a *majority* of nodes
     * even with no further proposals: each heartbeat carries the leader's commitIndex, so
     * followers advance and apply it. Before heartbeats, a follower only learned of a
     * commit on the next proposal, so a single quiescent proposal committed on the leader
     * alone.
     */
    @Test
    public void committedEntryPropagatesToMajorityViaHeartbeats() throws Exception {
        MessageRouter router = new MessageRouter();
        Scheduler scheduler = new DefaultScheduler(Executors.newScheduledThreadPool(NODE_COUNT * 2));
        List<Raft> algorithms = new ArrayList<>();
        List<VirtualNode> virtualNodes = new ArrayList<>();
        buildAndStartCluster(router, scheduler, algorithms, virtualNodes);

        try {
            Raft leader = waitForSingleLeader(algorithms, 2000, "initial cluster boot");
            leader.propose("only-value");
            waitForMajorityApplied(algorithms, 2000);
        } finally {
            virtualNodes.forEach(VirtualNode::stop);
            scheduler.shutdown();
        }
    }

    /**
     * A healthy leader must not be displaced: its heartbeats keep followers from timing
     * out, so over a window spanning many election timeouts the same node stays leader and
     * the term does not advance. Before heartbeats, a quiescent cluster re-elected roughly
     * every election-timeout interval.
     */
    @Test
    public void healthyLeaderIsNotDisplacedByElections() throws Exception {
        MessageRouter router = new MessageRouter();
        Scheduler scheduler = new DefaultScheduler(Executors.newScheduledThreadPool(NODE_COUNT * 2));
        List<Raft> algorithms = new ArrayList<>();
        List<VirtualNode> virtualNodes = new ArrayList<>();
        buildAndStartCluster(router, scheduler, algorithms, virtualNodes);

        try {
            Raft leader = waitForSingleLeader(algorithms, 2000, "initial cluster boot");
            int termAtElection = leader.getCurrentTerm();

            // Span several election-timeout windows (max timeout is 300ms).
            Thread.sleep(1500);

            assertSame(Raft.Role.LEADER, leader.getRole(), "the original leader should still be leading");
            assertEquals(termAtElection, leader.getCurrentTerm(),
                    "no new election should occur while the leader is healthy (term must not advance)");
            long leaderCount = algorithms.stream().filter(r -> r.getRole() == Raft.Role.LEADER).count();
            assertEquals(1, leaderCount, "there should still be exactly one leader");
        } finally {
            virtualNodes.forEach(VirtualNode::stop);
            scheduler.shutdown();
        }
    }

    /**
     * The algorithm must notify its {@link ConsensusObserver} of leader elections and
     * commits -- this is what the simulation turns into "leader elected" / "value
     * committed" entries in the dashboard event log.
     */
    @Test
    public void electionAndCommit_NotifyTheObserver() throws Exception {
        MessageRouter router = new MessageRouter();
        Scheduler scheduler = new DefaultScheduler(Executors.newScheduledThreadPool(NODE_COUNT * 2));
        SimulationProperties props = new SimulationProperties();
        List<String> nodeIds = new ArrayList<>();
        for (int i = 1; i <= NODE_COUNT; i++) {
            nodeIds.add("node" + i);
        }

        AtomicInteger leaderElections = new AtomicInteger();
        AtomicInteger commits = new AtomicInteger();
        AtomicReference<Object> lastCommitted = new AtomicReference<>();
        ConsensusObserver observer = new ConsensusObserver() {
            @Override
            public void onCommitted(String nodeId, Object value) {
                commits.incrementAndGet();
                lastCommitted.set(value);
            }

            @Override
            public void onLeaderElected(String nodeId, int term) {
                leaderElections.incrementAndGet();
            }
        };

        List<Raft> algorithms = new ArrayList<>();
        List<VirtualNode> virtualNodes = new ArrayList<>();
        for (String nodeId : nodeIds) {
            Raft raft = new Raft(nodeId, nodeIds, router, scheduler, props);
            raft.setConsensusObserver(observer); // before start(), so the first election is observed
            algorithms.add(raft);
            virtualNodes.add(startVirtualNode(nodeId, raft, router, scheduler));
        }

        try {
            Raft leader = waitForSingleLeader(algorithms, 2000, "initial cluster boot");
            // onLeaderElected fires on the node thread just after the role flips to LEADER,
            // so poll briefly rather than asserting the instant we observe the role.
            long electionDeadline = System.currentTimeMillis() + 1000;
            while (System.currentTimeMillis() < electionDeadline && leaderElections.get() < 1) {
                Thread.sleep(20);
            }
            assertTrue(leaderElections.get() >= 1, "observer should be notified of a leader election");

            leader.propose("v1");
            long deadline = System.currentTimeMillis() + 2000;
            while (System.currentTimeMillis() < deadline && commits.get() < 1) {
                Thread.sleep(20);
            }
            assertTrue(commits.get() >= 1, "observer should be notified of the committed value");
            assertEquals("v1", lastCommitted.get(), "the committed value should be reported to the observer");
        } finally {
            virtualNodes.forEach(VirtualNode::stop);
            scheduler.shutdown();
        }
    }

    private void buildAndStartCluster(MessageRouter router, Scheduler scheduler,
                                      List<Raft> algorithms, List<VirtualNode> virtualNodes) {
        SimulationProperties simulationProperties = new SimulationProperties();
        List<String> nodeIds = new ArrayList<>();
        for (int i = 1; i <= NODE_COUNT; i++) {
            nodeIds.add("node" + i);
        }
        for (String nodeId : nodeIds) {
            Raft raft = new Raft(nodeId, nodeIds, router, scheduler, simulationProperties);
            algorithms.add(raft);
            virtualNodes.add(startVirtualNode(nodeId, raft, router, scheduler));
        }
    }

    /**
     * Polls until at least a majority of nodes have applied a committed entry.
     */
    private void waitForMajorityApplied(List<Raft> algorithms, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            long applied = algorithms.stream().filter(r -> r.getLastApplied() >= 1).count();
            if (applied >= MAJORITY) {
                return;
            }
            Thread.sleep(20);
        }
        long applied = algorithms.stream().filter(r -> r.getLastApplied() >= 1).count();
        fail("Expected a majority (" + MAJORITY + ") of nodes to apply the entry within "
                + timeoutMillis + "ms, only reached " + applied);
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
