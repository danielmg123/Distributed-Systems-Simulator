package com.dss.backend.consensus.raft;

import com.dss.backend.consensus.AbstractConsensusAlgorithm;
import com.dss.backend.consensus.util.ConsensusBroadcaster;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.SimulationMessageFactory;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A basic Raft consensus algorithm implementation with log replication.
 * This implementation extends AbstractConsensusAlgorithm to inherit default
 * (no-op) implementations for methods such as accept(), which are not used in Raft.
 *
 * Note: This is a simplified version. In a full implementation, additional logic
 * (such as timer-based elections and more robust log matching) would be required.
 */
public class Raft extends AbstractConsensusAlgorithm {

    private final AppLogger appLogger = new DefaultAppLogger(Raft.class);

    // Raft roles.
    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    // Persistent state on all servers.
    private volatile int currentTerm = 0;
    private volatile String votedFor = null;
    // In-memory log entries.
    private final List<LogEntry> log = new ArrayList<>();

    // Volatile state on all servers.
    private volatile int commitIndex = 0;
    private volatile int lastApplied = 0;

    // Volatile state on leaders.
    private final Map<String, Integer> nextIndexMap = new ConcurrentHashMap<>();
    private final Map<String, Integer> matchIndexMap = new ConcurrentHashMap<>();

    // Identification.
    private final String myNodeId;
    private final List<String> allNodeIds;
    private final MessageRouter router;

    // Election-related state.
    private volatile Role role = Role.FOLLOWER;
    private final Map<Integer, Integer> votesReceivedPerTerm = new ConcurrentHashMap<>();

    // For broadcasting messages to other nodes.
    private final ConsensusBroadcaster broadcaster;

    /**
     * Constructor for Raft.
     *
     * @param nodeId     The unique ID of this node.
     * @param allNodeIds A list of all node IDs in the cluster.
     * @param router     The shared MessageRouter for message routing.
     */
    public Raft(String nodeId, List<String> allNodeIds, MessageRouter router) {
        this.myNodeId = nodeId;
        this.allNodeIds = allNodeIds;
        this.router = router;
        this.broadcaster = new ConsensusBroadcaster(router, myNodeId);
    }

    /**
     * Proposes a new command. Only the leader can append new commands to its log.
     *
     * @param value The command or value to propose.
     */
    @Override
    public void propose(Object value) {
        if (role != Role.LEADER) {
            appLogger.info("Raft Node {} is not LEADER; ignoring propose() or forwarding to leader.", myNodeId);
            return;
        }
        // Append the new command as a log entry with the current term.
        LogEntry entry = new LogEntry(currentTerm, value);
        log.add(entry);
        int newEntryIndex = log.size() - 1;
        appLogger.info("Leader {} appended new command at index={} for term={}", myNodeId, newEntryIndex, currentTerm);

        // Leader is always fully caught up.
        matchIndexMap.put(myNodeId, newEntryIndex);

        // Replicate new log entries to all followers.
        broadcastAppendEntries();
    }

    /**
     * The accept() method is not used directly in Raft; the default no-op is inherited.
     */

    /**
     * Commits the command by applying it locally. Additional application logic may be added here.
     *
     * @param value The command/value being committed.
     */
    @Override
    public void commit(Object value) {
        appLogger.info("Raft Node {} commits: {}", myNodeId, value);
        // Application-specific commit logic could be placed here.
    }

    /**
     * Handles incoming messages. Expects payloads to be instances of RaftPayload.
     *
     * @param msg The incoming SimulationMessage.
     */
    @Override
    public void handleMessage(SimulationMessage msg) {
        if (!(msg.getPayload() instanceof RaftPayload rp)) {
            return;
        }
        switch (rp.getType()) {
            case REQUEST_VOTE -> handleRequestVote(msg.getSourceNodeId(), rp);
            case REQUEST_VOTE_RESPONSE -> handleRequestVoteResponse(msg.getSourceNodeId(), rp);
            case APPEND_ENTRIES -> handleAppendEntries(msg.getSourceNodeId(), rp);
            case APPEND_ENTRIES_RESPONSE -> handleAppendEntriesResponse(msg.getSourceNodeId(), rp);
            default -> appLogger.debug("Raft: Unhandled message type: {}", rp.getType());
        }
    }

    // ---------------------- Election Methods ----------------------

    /**
     * Initiates a new election by transitioning to candidate and requesting votes.
     */
    public void triggerElection() {
        becomeCandidate();
        requestVotesFromPeers();
    }

    private void becomeCandidate() {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor = myNodeId;
        votesReceivedPerTerm.put(currentTerm, 1);
        appLogger.info("{} becomes CANDIDATE for term {}", myNodeId, currentTerm);
    }

    private void requestVotesFromPeers() {
        RaftPayload voteRequest = new RaftPayload();
        voteRequest.setType(MessageType.REQUEST_VOTE);
        voteRequest.setTerm(currentTerm);
        voteRequest.setCandidateId(myNodeId);

        // Broadcast vote request to all nodes.
        broadcaster.broadcast(MessageType.REQUEST_VOTE, voteRequest);
    }

    private void handleRequestVote(String sourceNode, RaftPayload rp) {
        int term = rp.getTerm();
        String candidateId = rp.getCandidateId();

        if (term > currentTerm) {
            becomeFollower(term);
        }

        boolean grantVote = false;
        if (term == currentTerm && (votedFor == null || votedFor.equals(candidateId))) {
            grantVote = true;
            votedFor = candidateId;
        }

        RaftPayload response = new RaftPayload();
        response.setType(MessageType.REQUEST_VOTE_RESPONSE);
        response.setTerm(currentTerm);
        response.setVoteGranted(grantVote);

        SimulationMessage sm = SimulationMessageFactory.createMessage(myNodeId, sourceNode, MessageType.REQUEST_VOTE_RESPONSE, response);
        router.messageSent(sm);
    }

    private void handleRequestVoteResponse(String sourceNode, RaftPayload rp) {
        int term = rp.getTerm();
        boolean voteGranted = rp.isVoteGranted();

        if (term > currentTerm) {
            becomeFollower(term);
            return;
        }

        if (role == Role.CANDIDATE && term == currentTerm && voteGranted) {
            int voteCount = votesReceivedPerTerm.getOrDefault(currentTerm, 0) + 1;
            votesReceivedPerTerm.put(currentTerm, voteCount);
            if (voteCount >= ((allNodeIds.size() / 2) + 1)) {
                becomeLeader();
            }
        }
    }

    private void becomeFollower(int newTerm) {
        appLogger.info("{} becomes FOLLOWER in term {}", myNodeId, newTerm);
        role = Role.FOLLOWER;
        currentTerm = newTerm;
        votedFor = null;
    }

    private void becomeLeader() {
        role = Role.LEADER;
        appLogger.info("{} is now LEADER in term {}", myNodeId, currentTerm);

        int lastLogIndex = log.size() - 1;
        for (String nodeId : allNodeIds) {
            nextIndexMap.put(nodeId, lastLogIndex + 1);
            matchIndexMap.put(nodeId, -1);
        }
        matchIndexMap.put(myNodeId, lastLogIndex);

        // Immediately send heartbeats to followers.
        broadcastAppendEntries();
    }

    // ---------------------- Log Replication Methods ----------------------

    private void broadcastAppendEntries() {
        if (role != Role.LEADER) return;

        int lastLogIndex = log.size() - 1;
        for (String follower : allNodeIds) {
            if (follower.equals(myNodeId)) continue;
            sendAppendEntriesTo(follower);
        }
    }

    private void sendAppendEntriesTo(String follower) {
        int nextIndex = nextIndexMap.getOrDefault(follower, log.size());
        List<LogEntry> newEntries = new ArrayList<>();
        if (nextIndex < log.size()) {
            newEntries = log.subList(nextIndex, log.size());
        }
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

        SimulationMessage sm = SimulationMessageFactory.createMessage(myNodeId, follower, MessageType.APPEND_ENTRIES, payload);
        router.messageSent(sm);
    }

    private void handleAppendEntries(String sourceNode, RaftPayload rp) {
        int leaderTerm = rp.getTerm();

        if (leaderTerm > currentTerm) {
            becomeFollower(leaderTerm);
        }
        if (leaderTerm < currentTerm) {
            sendAppendEntriesResponse(false, log.size(), -1, sourceNode);
            return;
        }
        if (role != Role.FOLLOWER) {
            role = Role.FOLLOWER;
        }

        int prevLogIndex = rp.getPrevLogIndex();
        int prevLogTerm = rp.getPrevLogTerm();

        if (prevLogIndex >= 0) {
            if (prevLogIndex >= log.size() || log.get(prevLogIndex).getTerm() != prevLogTerm) {
                int matchIdx = Math.min(prevLogIndex, log.size());
                sendAppendEntriesResponse(false, matchIdx, (prevLogIndex < log.size()) ? log.get(prevLogIndex).getTerm() : -1, sourceNode);
                return;
            }
        }

        List<LogEntry> entries = rp.getEntries();
        int currentIndex = prevLogIndex + 1;
        for (LogEntry newEntry : entries) {
            if (currentIndex < log.size()) {
                LogEntry existing = log.get(currentIndex);
                if (existing.getTerm() != newEntry.getTerm()) {
                    while (log.size() > currentIndex) {
                        log.remove(log.size() - 1);
                    }
                }
            }
            if (currentIndex >= log.size()) {
                log.add(newEntry);
            }
            currentIndex++;
        }

        if (rp.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(rp.getLeaderCommit(), log.size() - 1);
            applyEntries();
        }

        int lastAppended = currentIndex - 1;
        sendAppendEntriesResponse(true, lastAppended, -1, sourceNode);
    }

    private void applyEntries() {
        while (lastApplied < commitIndex + 1) {
            LogEntry entry = log.get(lastApplied);
            lastApplied++;
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

        SimulationMessage sm = SimulationMessageFactory.createMessage(myNodeId, targetNode, MessageType.APPEND_ENTRIES_RESPONSE, rp);
        router.messageSent(sm);
    }

    private void handleAppendEntriesResponse(String follower, RaftPayload rp) {
        if (role != Role.LEADER) return;

        if (rp.getTerm() > currentTerm) {
            becomeFollower(rp.getTerm());
            return;
        }

        boolean success = rp.isSuccess();
        if (!success) {
            int fallbackIndex = rp.getMatchIndex();
            int oldNext = nextIndexMap.get(follower);
            int newNext = Math.min(oldNext - 1, fallbackIndex);
            newNext = Math.max(newNext, 0);
            nextIndexMap.put(follower, newNext);
            sendAppendEntriesTo(follower);
        } else {
            int matchIndex = rp.getMatchIndex();
            matchIndexMap.put(follower, matchIndex);
            nextIndexMap.put(follower, matchIndex + 1);

            for (int i = log.size() - 1; i > commitIndex; i--) {
                int replicatedCount = 1; // Counting the leader.
                for (String nodeId : allNodeIds) {
                    int m = matchIndexMap.getOrDefault(nodeId, -1);
                    if (m >= i) {
                        replicatedCount++;
                    }
                }
                if (replicatedCount >= ((allNodeIds.size() / 2) + 1)
                        && log.get(i).getTerm() == currentTerm) {
                    commitIndex = i;
                    applyEntries();
                    break;
                }
            }
        }
    }
}