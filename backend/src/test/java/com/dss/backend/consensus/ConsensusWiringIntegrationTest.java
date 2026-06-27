package com.dss.backend.consensus;

import com.dss.backend.config.SimulationProperties;
import com.dss.backend.consensus.multi_paxos.MultiPaxos;
import com.dss.backend.consensus.paxos.PaxosAlgorithm;
import com.dss.backend.consensus.raft.Raft;
import com.dss.backend.engine.DefaultScheduler;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.VirtualNode;
import com.dss.backend.model.ConsensusAlgorithmType;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import com.dss.backend.model.SimulationConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Wires a small cluster the same way the running application does -- algorithms built by
 * the real {@link ConsensusAlgorithmFactory}, wrapped in {@link VirtualNode}s on a shared
 * {@link MessageRouter} and a real {@link DefaultScheduler}, with proposals submitted
 * through {@code VirtualNode.propose()} -- and checks that a proposed value is actually
 * agreed on a quorum (not merely sent).
 * <p>
 * This is the coverage the per-algorithm unit tests lacked: they configured each
 * algorithm by hand and asserted on individual message exchanges. These tests go through
 * the factory and the node's real processing thread, which is where the earlier wiring
 * bugs lived.
 */
public class ConsensusWiringIntegrationTest {

    private static final int NODE_COUNT = 3;
    private static final int QUORUM = NODE_COUNT / 2 + 1;

    @Test
    public void raft_proposedValueCommitsOnMajority() throws Exception {
        Cluster cluster = new Cluster(ConsensusAlgorithmType.RAFT);
        try {
            // A leader has to emerge before a proposal can be replicated.
            int leaderIdx = waitForRaftLeader(cluster, 3000);
            cluster.nodes.get(leaderIdx).propose("v1");

            // commitIndex >= 0 means the single entry (index 0) is actually committed.
            waitForQuorum(cluster, a -> ((Raft) a).getCommitIndex() >= 0,
                    "a majority to commit the proposed entry");
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    public void paxos_proposedValueIsChosenByMajority() throws Exception {
        Cluster cluster = new Cluster(ConsensusAlgorithmType.PAXOS);
        try {
            // Paxos is leaderless; any node can drive a round.
            cluster.nodes.get(0).propose("v1");
            waitForQuorum(cluster, a -> "v1".equals(((PaxosAlgorithm) a).getChosenValue()),
                    "a majority to learn the chosen value");
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    public void multiPaxos_proposedValueCommitsOnMajority() throws Exception {
        Cluster cluster = new Cluster(ConsensusAlgorithmType.MULTI_PAXOS);
        try {
            // The factory designates the first node as the Multi-Paxos leader.
            cluster.nodes.get(0).propose("v1");
            waitForQuorum(cluster, a -> "v1".equals(((MultiPaxos) a).getCommittedValue()),
                    "a majority to commit the proposed value");
        } finally {
            cluster.shutdown();
        }
    }

    /** A cluster of NODE_COUNT nodes built through the real factory and started. */
    private static final class Cluster {
        final MessageRouter router;
        final Scheduler scheduler;
        final List<ConsensusAlgorithm> algorithms = new ArrayList<>();
        final List<VirtualNode> nodes = new ArrayList<>();

        Cluster(ConsensusAlgorithmType type) {
            this.scheduler = new DefaultScheduler(Executors.newScheduledThreadPool(NODE_COUNT * 2));
            this.router = new MessageRouter();

            List<String> nodeIds = new ArrayList<>();
            for (int i = 1; i <= NODE_COUNT; i++) {
                nodeIds.add("node" + i);
            }

            SimulationConfig config = new SimulationConfig();
            config.setAlgorithmType(type);

            ConsensusAlgorithmFactory factory =
                    new ConsensusAlgorithmFactory(router, scheduler, new SimulationProperties());

            for (String nodeId : nodeIds) {
                ConsensusAlgorithm algorithm = factory.createAlgorithm(nodeId, nodeIds, config);
                Node node = new Node();
                node.setId(nodeId);
                node.setStatus(NodeStatus.ACTIVE);
                VirtualNode vNode = new VirtualNode(node, algorithm, router, scheduler);
                vNode.start();
                router.registerNode(nodeId, vNode);
                algorithms.add(algorithm);
                nodes.add(vNode);
            }
        }

        void shutdown() {
            nodes.forEach(VirtualNode::stop);
            scheduler.shutdown();
        }
    }

    private int waitForRaftLeader(Cluster cluster, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < cluster.algorithms.size(); i++) {
                if (((Raft) cluster.algorithms.get(i)).getRole() == Raft.Role.LEADER) {
                    return i;
                }
            }
            Thread.sleep(20);
        }
        fail("Expected a Raft leader to be elected within " + timeoutMillis + "ms");
        return -1; // unreachable
    }

    private void waitForQuorum(Cluster cluster, Predicate<ConsensusAlgorithm> condition, String what)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            long count = cluster.algorithms.stream().filter(condition).count();
            if (count >= QUORUM) {
                return;
            }
            Thread.sleep(25);
        }
        long count = cluster.algorithms.stream().filter(condition).count();
        fail("Expected " + what + " (>= " + QUORUM + " of " + NODE_COUNT + "), only reached " + count);
    }
}
