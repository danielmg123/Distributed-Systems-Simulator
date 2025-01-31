package com.dss.backend.algorithm.consensus.raft;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.SimulationMessage;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A minimal skeleton for Raft consensus:
 *  - Leader election
 *  - Heartbeats (AppendEntries)
 *  - Basic log structure (incomplete for actual replication)
 *
 * This example shows how you'd track currentTerm, votedFor, role, and handle
 * the main RPCs: RequestVote and AppendEntries. Real log replication details
 * are greatly simplified here.
 *
 * NOTE: For concurrency, you’ll likely rely on your VirtualNodeThread and
 * handleMessage() calls just like Paxos. This skeleton does minimal
 * timeouts or scheduling. In a real environment, you'd have to simulate
 * timers for election, heartbeats, etc.
 */
@Component
public class Raft implements ConsensusAlgorithm {

    // ------------------------------
    // Raft Roles
    // ------------------------------
    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    // ------------------------------
    // Basic Raft Node State
    // ------------------------------
    private volatile int currentTerm = 0;     // latest term server has seen
    private volatile String votedFor = null;  // candidateId that received vote in current term
    private volatile Role role = Role.FOLLOWER;

    // Minimal in-memory "log"
    // Real Raft would keep a list of LogEntries {term, index, command}
    // We'll skip the actual commands for brevity
    private final AtomicInteger commitIndex = new AtomicInteger(0);

    // The ID of this node, plus the entire cluster membership
    private final String myNodeId;
    private final List<String> allNodeIds;

    // We need to route messages to other nodes
    private final MessageRouter router;

    // For leader election, track how many votes we've gotten this term
    private final Map<Integer, Integer> votesReceivedPerTerm = new ConcurrentHashMap<>();


    public Raft(String nodeId, List<String> allNodeIds, MessageRouter router) {
        this.myNodeId = nodeId;
        this.allNodeIds = allNodeIds;
        this.router = router;
    }

    /**
     * This no-arg constructor is used by Spring if you have @Component scanning.
     * You’d need a setter or separate init method to populate nodeId, allNodeIds, etc.
     * Alternatively, remove the @Component annotation and instantiate Raft yourself.
     */
    public Raft() {
        // This leaves everything null or zero. Not good for real usage but helps avoid
        // a bean creation error if you do @Autowired somewhere. 
        // For a real system, you'd remove or handle properly.
        this.myNodeId = null;
        this.allNodeIds = null;
        this.router = null;
    }

    // ------------------------------
    // Implementation of ConsensusAlgorithm
    // ------------------------------
    @Override
    public void propose(Object value) {
        // In Raft, new commands are typically appended to the leader’s log.
        // If we’re the leader, we can replicate. If not, we forward or ignore.
        if (role == Role.LEADER) {
            // Example: append "value" to our local log
            System.out.println("Raft Node " + myNodeId + " received propose() for " + value + " as LEADER");
            // In real Raft, you’d store the entry, then replicate via AppendEntries.
            // For now, we’ll just fake it:
            replicateLogEntry(value);
        } else {
            System.out.println("Raft Node " + myNodeId + " is not LEADER; ignoring propose() or forwarding to leader.");
            // We could forward to known leader if we track it. 
        }
    }

    @Override
    public boolean accept(Object proposal) {
        // In Raft, accept() isn’t exactly used. 
        // Accepting log entries is done in handleAppendEntries.
        return false;
    }

    @Override
    public void commit(Object value) {
        // In real Raft, we "commit" a log entry once it's safely replicated on a majority.
        // For a skeleton, let's just print:
        System.out.println("Raft Node " + myNodeId + " commits: " + value);
        commitIndex.incrementAndGet(); // pretend each commit increments the index
    }

    // ------------------------------
    // Message Handling
    // (Typically invoked by VirtualNodeThread)
    // ------------------------------
    public void handleMessage(SimulationMessage msg) {
        if (msg.getPayload() instanceof RaftPayload rp) {
            switch (rp.getType()) {
                case REQUEST_VOTE -> handleRequestVote(msg.getSourceNodeId(), rp);
                case REQUEST_VOTE_RESPONSE -> handleRequestVoteResponse(msg.getSourceNodeId(), rp);
                case APPEND_ENTRIES -> handleAppendEntries(msg.getSourceNodeId(), rp);
                case APPEND_ENTRIES_RESPONSE -> handleAppendEntriesResponse(msg.getSourceNodeId(), rp);
                default -> {
                    // Unknown message or not implemented
                }
            }
        }
    }

    // Leader, Follower, or Candidate states

    // ------------------------------
    // Election Start
    // ------------------------------
    private void startElection() {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor = myNodeId;

        // Reset or set votes received for this new term
        votesReceivedPerTerm.put(currentTerm, 1); // I voted for myself

        System.out.println(myNodeId + " starting election for term " + currentTerm);

        // Send RequestVote to all other nodes
        for (String nodeId : allNodeIds) {
            if (!nodeId.equals(myNodeId)) {
                RaftPayload voteRequest = new RaftPayload();
                voteRequest.setType(MessageType.REQUEST_VOTE);
                voteRequest.setTerm(currentTerm);
                voteRequest.setCandidateId(myNodeId);

                SimulationMessage sm = new SimulationMessage(
                        myNodeId,
                        nodeId,
                        MessageType.PROPOSAL,   // or define a custom RAFT type if you prefer
                        voteRequest
                );
                router.messageSent(sm);
            }
        }
    }

    // ------------------------------
    // RequestVote RPC
    // ------------------------------
    private void handleRequestVote(String sourceNode, RaftPayload rp) {
        int term = rp.getTerm();
        String candidateId = rp.getCandidateId();

        if (term > currentTerm) {
            // Found a higher term, so become a follower
            becomeFollower(term);
        }

        boolean grantVote = false;

        // Check if we can vote:
        if (term == currentTerm && (votedFor == null || votedFor.equals(candidateId))) {
            // For simplicity, ignoring log indices or "up-to-date" checks
            // We can grant the vote
            grantVote = true;
            votedFor = candidateId;
        }

        // Build response
        RaftPayload response = new RaftPayload();
        response.setType(MessageType.REQUEST_VOTE_RESPONSE);
        response.setTerm(currentTerm);
        response.setVoteGranted(grantVote);

        // Send back
        SimulationMessage sm = new SimulationMessage(
                myNodeId,
                sourceNode,
                MessageType.PROPOSAL,
                response
        );
        router.messageSent(sm);
    }

    private void handleRequestVoteResponse(String sourceNode, RaftPayload rp) {
        int term = rp.getTerm();
        boolean voteGranted = rp.isVoteGranted();

        // If we see a higher term, we revert to follower
        if (term > currentTerm) {
            becomeFollower(term);
            return;
        }

        // If still a candidate, count the vote
        if (role == Role.CANDIDATE && term == currentTerm && voteGranted) {
            int voteCount = votesReceivedPerTerm.getOrDefault(currentTerm, 0);
            voteCount++;
            votesReceivedPerTerm.put(currentTerm, voteCount);

            if (voteCount >= (allNodeIds.size() / 2) + 1) {
                // Achieved majority => become leader
                becomeLeader();
            }
        }
    }

    // ------------------------------
    // AppendEntries RPC (heartbeats + log replication)
    // ------------------------------
    private void handleAppendEntries(String sourceNode, RaftPayload rp) {
        int leaderTerm = rp.getTerm();

        // If leaderTerm is higher, we step down
        if (leaderTerm > currentTerm) {
            becomeFollower(leaderTerm);
        }

        // If the leader’s term < our term, we reject
        if (leaderTerm < currentTerm) {
            sendAppendEntriesResponse(false, sourceNode);
            return;
        }

        // We’re a follower if we see a valid leader
        if (role != Role.FOLLOWER) {
            // Step down if we aren’t follower
            role = Role.FOLLOWER;
        }

        // “Heartbeat” => success
        sendAppendEntriesResponse(true, sourceNode);
    }

    private void handleAppendEntriesResponse(String sourceNode, RaftPayload rp) {
        // If we are leader, we can track who is “caught up,” etc.
        // If success, we might advance nextIndex for that follower. 
        // This skeleton just prints a debug line
        if (role == Role.LEADER) {
            boolean success = rp.isSuccess();
            System.out.println("Leader " + myNodeId + " got AppendEntriesResponse from "
                    + sourceNode + ": success=" + success);
        }
    }

    // ------------------------------
    // Helper: becomeFollower
    // ------------------------------
    private void becomeFollower(int newTerm) {
        System.out.println(myNodeId + " becomes FOLLOWER in term " + newTerm);
        role = Role.FOLLOWER;
        currentTerm = newTerm;
        votedFor = null; // reset
    }

    // ------------------------------
    // Helper: becomeLeader
    // ------------------------------
    private void becomeLeader() {
        role = Role.LEADER;
        System.out.println(myNodeId + " is now LEADER in term " + currentTerm);

        // Immediately send heartbeats (AppendEntries) to all
        sendHeartbeats();
    }

    // ------------------------------
    // Helper: replicateLogEntry
    // (skeleton for demonstration)
    // ------------------------------
    private void replicateLogEntry(Object value) {
        // In real Raft, we’d add the entry to our local log, then send AppendEntries
        // with the new entry. Here, we’ll just do a “heartbeat” to indicate a new command.
        for (String nodeId : allNodeIds) {
            if (!nodeId.equals(myNodeId)) {
                RaftPayload rp = new RaftPayload();
                rp.setType(MessageType.APPEND_ENTRIES);
                rp.setTerm(currentTerm);
                rp.setLeaderId(myNodeId);
                rp.setEntry(value); // pretend this is the “log entry”

                SimulationMessage sm = new SimulationMessage(
                        myNodeId,
                        nodeId,
                        MessageType.PROPOSAL,
                        rp
                );
                router.messageSent(sm);
            }
        }
        // We’d then wait for majority “AppendEntriesResponse” success to commit the entry.
        // For simplicity, let’s call commit immediately (NOT real Raft).
        commit(value);
    }

    // ------------------------------
    // Helper: sendHeartbeats
    // (Would be on a timer in real Raft)
    // ------------------------------
    private void sendHeartbeats() {
        for (String nodeId : allNodeIds) {
            if (!nodeId.equals(myNodeId)) {
                RaftPayload rp = new RaftPayload();
                rp.setType(MessageType.APPEND_ENTRIES);
                rp.setTerm(currentTerm);
                rp.setLeaderId(myNodeId);

                SimulationMessage sm = new SimulationMessage(
                        myNodeId,
                        nodeId,
                        MessageType.PROPOSAL,
                        rp
                );
                router.messageSent(sm);
            }
        }
    }

    private void sendAppendEntriesResponse(boolean success, String targetNode) {
        RaftPayload rp = new RaftPayload();
        rp.setType(MessageType.APPEND_ENTRIES_RESPONSE);
        rp.setTerm(currentTerm);
        rp.setSuccess(success);

        SimulationMessage sm = new SimulationMessage(
                myNodeId,
                targetNode,
                MessageType.PROPOSAL,
                rp
        );
        router.messageSent(sm);
    }

    // ------------------------------
    // Additional Exposed Methods
    // for Testing or Manual Triggers
    // ------------------------------
    public void triggerElection() {
        // If we’re a follower or an out-of-date leader, we might start an election
        startElection();
    }

    public void triggerHeartbeat() {
        if (role == Role.LEADER) {
            sendHeartbeats();
        }
    }
}
