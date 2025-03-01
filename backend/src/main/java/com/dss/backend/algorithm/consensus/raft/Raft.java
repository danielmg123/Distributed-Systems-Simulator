package com.dss.backend.algorithm.consensus.raft;

import com.dss.backend.algorithm.consensus.ConsensusAlgorithm;
import com.dss.backend.engine.concurrent.MessageRouter;
import com.dss.backend.engine.concurrent.MessageType;
import com.dss.backend.engine.concurrent.SimulationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A more complete Raft example with basic log replication.
 * Does not include timer-based election logic.
 */
@Component
public class Raft implements ConsensusAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(Raft.class);

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
    private volatile int currentTerm = 0;   
    private volatile String votedFor = null;
    private volatile Role role = Role.FOLLOWER;

    // Our in-memory log of entries:
    private final List<LogEntry> log = new ArrayList<>();

    // The highest log index known to be committed
    private volatile int commitIndex = 0;

    // The highest log index that this server has applied to its state machine
    private volatile int lastApplied = 0;

    // For leader: track nextIndex and matchIndex for each follower
    private final Map<String, Integer> nextIndexMap = new ConcurrentHashMap<>();
    private final Map<String, Integer> matchIndexMap = new ConcurrentHashMap<>();

    // The ID of this node, plus the entire cluster membership
    private final String myNodeId;
    private final List<String> allNodeIds;

    // We need to route messages to other nodes
    private final MessageRouter router;

    // For leader election, track how many votes we've gotten this term
    private final Map<Integer, Integer> votesReceivedPerTerm = new ConcurrentHashMap<>();

    // Keep track of whether we are alive or just constructed
    private boolean initialized = false;

    public Raft(String nodeId, List<String> allNodeIds, MessageRouter router) {
        this.myNodeId = nodeId;
        this.allNodeIds = allNodeIds;
        this.router = router;
    }

    /**
     * No-arg constructor for Spring bean scanning; we override with our own init method.
     */
    public Raft() {
        this.myNodeId = null;
        this.allNodeIds = null;
        this.router = null;
    }

    @Override
    public void propose(Object value) {
        // Only the leader accepts new commands
        if (role != Role.LEADER) {
            logger.info("Raft Node {} is not LEADER; ignoring propose() or forwarding to leader.", myNodeId);
            return;
        }
        // Append to local log (term = currentTerm)
        LogEntry entry = new LogEntry(currentTerm, value);
        log.add(entry);
        int newEntryIndex = log.size() - 1;

        logger.info("Leader {} appended new command at index={} for term={}", myNodeId, newEntryIndex, currentTerm);

        // Update nextIndex/matchIndex for this leader itself
        // In normal Raft, leader always “matchIndex = lastLogIndex” for self
        matchIndexMap.put(myNodeId, newEntryIndex);

        // Replicate to followers
        broadcastAppendEntries();
    }

    @Override
    public boolean accept(Object proposal) {
        // Unused direct accept method in Raft
        return false;
    }

    @Override
    public void commit(Object value) {
        // If we wanted an external callback each time we commit, we could do so here.
        logger.info("Raft Node {} commits: {}", myNodeId, value);
    }

    @Override
    public void handleMessage(SimulationMessage msg) {
        if (!(msg.getPayload() instanceof RaftPayload rp)) {
            return; // Not a Raft message
        }
        switch (rp.getType()) {
            case REQUEST_VOTE -> handleRequestVote(msg.getSourceNodeId(), rp);
            case REQUEST_VOTE_RESPONSE -> handleRequestVoteResponse(msg.getSourceNodeId(), rp);
            case APPEND_ENTRIES -> handleAppendEntries(msg.getSourceNodeId(), rp);
            case APPEND_ENTRIES_RESPONSE -> handleAppendEntriesResponse(msg.getSourceNodeId(), rp);
            default -> { /* ignore or log */ }
        }
    }

    // -------------- ELECTIONS --------------
    public void triggerElection() {
        // If we’re a follower or an out-of-date leader, start a new election
        becomeCandidate();
        requestVotesFromPeers();
    }

    private void becomeCandidate() {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor = myNodeId;
        votesReceivedPerTerm.put(currentTerm, 1);
        logger.info("{} becomes CANDIDATE for term {}", myNodeId, currentTerm);
    }

    private void requestVotesFromPeers() {
        // In a real system we’d include lastLogIndex/lastLogTerm in the RequestVote
        for (String nodeId : allNodeIds) {
            if (!nodeId.equals(myNodeId)) {
                RaftPayload voteRequest = new RaftPayload();
                voteRequest.setType(MessageType.REQUEST_VOTE);
                voteRequest.setTerm(currentTerm);
                voteRequest.setCandidateId(myNodeId);

                SimulationMessage sm = new SimulationMessage(
                    myNodeId,
                    nodeId,
                    MessageType.PROPOSAL, // or keep it as REQUEST_VOTE 
                    voteRequest
                );
                router.messageSent(sm);
            }
        }
    }

    private void handleRequestVote(String sourceNode, RaftPayload rp) {
        int term = rp.getTerm();
        String candidateId = rp.getCandidateId();

        if (term > currentTerm) {
            becomeFollower(term);
        }

        boolean grantVote = false;

        // Simple logic: if term matches and we haven’t voted yet (or votedFor is them), grant vote
        if (term == currentTerm && (votedFor == null || votedFor.equals(candidateId))) {
            grantVote = true;
            votedFor = candidateId;
        }

        // Build response
        RaftPayload response = new RaftPayload();
        response.setType(MessageType.REQUEST_VOTE_RESPONSE);
        response.setTerm(currentTerm);
        response.setVoteGranted(grantVote);

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

        if (term > currentTerm) {
            becomeFollower(term);
            return;
        }

        // If still candidate in this term
        if (role == Role.CANDIDATE && term == currentTerm && voteGranted) {
            int voteCount = votesReceivedPerTerm.getOrDefault(currentTerm, 0);
            voteCount++;
            votesReceivedPerTerm.put(currentTerm, voteCount);

            if (voteCount >= (allNodeIds.size() / 2) + 1) {
                becomeLeader();
            }
        }
    }

    private void becomeFollower(int newTerm) {
        logger.info("{} becomes FOLLOWER in term {}", myNodeId, newTerm);
        role = Role.FOLLOWER;
        currentTerm = newTerm;
        votedFor = null;
    }

    private void becomeLeader() {
        role = Role.LEADER;
        logger.info("{} is now LEADER in term {}", myNodeId, currentTerm);

        // Initialize nextIndex for each follower to leader’s lastLogIndex + 1
        int lastLogIndex = log.size() - 1; // could be -1 if no entries
        for (String nodeId : allNodeIds) {
            nextIndexMap.put(nodeId, lastLogIndex + 1);
            matchIndexMap.put(nodeId, -1);
        }
        matchIndexMap.put(myNodeId, lastLogIndex); // leader is always fully caught up

        // Immediately send heartbeats
        broadcastAppendEntries();
    }

    // -------------- APPEND ENTRIES (Log Replication) --------------
    private void broadcastAppendEntries() {
        if (role != Role.LEADER) return;

        int lastLogIndex = log.size() - 1;
        for (String follower : allNodeIds) {
            if (follower.equals(myNodeId)) continue;
            sendAppendEntriesTo(follower);
        }
    }

    /**
     * Sends an AppendEntries RPC to a single follower,
     * including any new entries that the follower is missing.
     */
    private void sendAppendEntriesTo(String follower) {
        int nextIndex = nextIndexMap.getOrDefault(follower, log.size());

        // The follower’s nextIndex points to the *next* entry we want them to have.
        // So the new entries are from nextIndex onward.
        List<LogEntry> newEntries = new ArrayList<>();
        if (nextIndex < log.size()) {
            newEntries = log.subList(nextIndex, log.size());
        }

        // prevLogIndex is nextIndex - 1
        int prevLogIndex = nextIndex - 1;
        int prevLogTerm = (prevLogIndex >= 0) ? log.get(prevLogIndex).getTerm() : -1;

        RaftPayload payload = new RaftPayload();
        payload.setType(MessageType.APPEND_ENTRIES);
        payload.setTerm(currentTerm);
        payload.setLeaderId(myNodeId);
        payload.setPrevLogIndex(prevLogIndex);
        payload.setPrevLogTerm(prevLogTerm);
        payload.setEntries(newEntries);
        payload.setLeaderCommit(commitIndex);

        SimulationMessage sm = new SimulationMessage(
            myNodeId,
            follower,
            MessageType.PROPOSAL,
            payload
        );
        router.messageSent(sm);
    }

    private void handleAppendEntries(String sourceNode, RaftPayload rp) {
        int leaderTerm = rp.getTerm();

        // Step down if the leader term is higher
        if (leaderTerm > currentTerm) {
            becomeFollower(leaderTerm);
        }
        // If the leader’s term < our term, reject
        if (leaderTerm < currentTerm) {
            sendAppendEntriesResponse(false, rp.getPrevLogIndex(), -1, sourceNode);
            return;
        }

        // We are a follower if we see a valid leader heartbeat
        if (role != Role.FOLLOWER) {
            role = Role.FOLLOWER;
        }

        // 1. Check the log matching property:
        int prevLogIndex = rp.getPrevLogIndex();
        int prevLogTerm = rp.getPrevLogTerm();

        if (prevLogIndex >= 0) {
            if (prevLogIndex >= log.size()) {
                // We don't even have prevLogIndex in our log
                sendAppendEntriesResponse(false, log.size(), -1, sourceNode);
                return;
            }
            if (log.get(prevLogIndex).getTerm() != prevLogTerm) {
                // Term mismatch
                sendAppendEntriesResponse(false, prevLogIndex, log.get(prevLogIndex).getTerm(), sourceNode);
                return;
            }
        }

        // 2. If valid, append new entries (handle conflicts)
        List<LogEntry> entries = rp.getEntries();
        int currentIndex = prevLogIndex + 1;

        for (LogEntry newEntry : entries) {
            // If there's an existing entry with same index but different term, delete it and all after it
            if (currentIndex < log.size()) {
                LogEntry existing = log.get(currentIndex);
                if (existing.getTerm() != newEntry.getTerm()) {
                    // Remove everything from currentIndex onward
                    while (log.size() > currentIndex) {
                        log.remove(log.size() - 1);
                    }
                }
            }
            // If we are missing this entry, append
            if (currentIndex >= log.size()) {
                log.add(newEntry);
            }
            currentIndex++;
        }

        // 3. Update commitIndex
        if (rp.getLeaderCommit() > commitIndex) {
            // commitIndex = min(leaderCommit, index of last new entry)
            commitIndex = Math.min(rp.getLeaderCommit(), log.size() - 1);
            applyEntries();
        }

        // 4. Respond success
        // matchIndex = index of the last entry we appended
        int lastAppended = currentIndex - 1;
        sendAppendEntriesResponse(true, lastAppended, -1, sourceNode);
    }

    private void applyEntries() {
        // Apply all entries up to commitIndex
        while (lastApplied < commitIndex + 1) {
            LogEntry entry = log.get(lastApplied);
            lastApplied++;
            // "Apply" to local state machine
            commit(entry.getCommand());
        }
    }

    private void sendAppendEntriesResponse(boolean success, int matchIndex, int conflictTerm, String targetNode) {
        RaftPayload rp = new RaftPayload();
        rp.setType(MessageType.APPEND_ENTRIES_RESPONSE);
        rp.setTerm(currentTerm);
        rp.setSuccess(success);
        rp.setMatchIndex(matchIndex);
        rp.setConflictTerm(conflictTerm);

        SimulationMessage sm = new SimulationMessage(
            myNodeId,
            targetNode,
            MessageType.PROPOSAL,
            rp
        );
        router.messageSent(sm);
    }

    private void handleAppendEntriesResponse(String follower, RaftPayload rp) {
        // Only the leader cares about these
        if (role != Role.LEADER) return;

        if (rp.getTerm() > currentTerm) {
            becomeFollower(rp.getTerm());
            return;
        }

        boolean success = rp.isSuccess();
        if (!success) {
            // Decrement nextIndex for that follower and retry
            int conflictTerm = rp.getConflictTerm();
            int fallbackIndex = rp.getMatchIndex(); // or log.size()
            // Simplified approach: just decrement nextIndex by 1
            // Real Raft would do more intelligent searching for conflictTerm
            int oldNext = nextIndexMap.get(follower);
            int newNext = Math.min(oldNext - 1, fallbackIndex);
            newNext = Math.max(newNext, 0);
            nextIndexMap.put(follower, newNext);
            sendAppendEntriesTo(follower);
        } else {
            // success = true, update matchIndex and nextIndex
            int matchIndex = rp.getMatchIndex();
            matchIndexMap.put(follower, matchIndex);
            nextIndexMap.put(follower, matchIndex + 1);

            // Check if we can advance commitIndex
            // For each i in [lastKnownLog..0], if i is “replicated” on majority, and log[i].term == currentTerm -> commit
            for (int i = log.size() - 1; i > commitIndex; i--) {
                int replicatedCount = 1; // counting leader
                for (String nodeId : allNodeIds) {
                    int m = matchIndexMap.getOrDefault(nodeId, -1);
                    if (m >= i) {
                        replicatedCount++;
                    }
                }
                if (replicatedCount >= (allNodeIds.size() / 2) + 1
                    && log.get(i).getTerm() == currentTerm) {
                    commitIndex = i;
                    applyEntries();
                    break;
                }
            }
        }
    }
}
