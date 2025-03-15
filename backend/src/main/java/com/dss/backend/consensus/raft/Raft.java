package com.dss.backend.consensus.raft;

import com.dss.backend.consensus.AbstractConsensusAlgorithm;
import com.dss.backend.consensus.util.ConsensusBroadcaster;
import com.dss.backend.consensus.util.ConsensusUtils;
import com.dss.backend.messaging.*;
import com.dss.backend.logging.AppLogger;
import com.dss.backend.logging.DefaultAppLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements the Raft consensus algorithm, providing leader election, log replication,
 * and safety guarantees in a distributed system.
 * <p>
 * <strong>Approach:</strong>
 * <ul>
 *   <li><em>Leader Election</em>:
 *       This implementation tracks the <code>currentTerm</code> and uses <code>REQUEST_VOTE</code>
 *       messages to gather votes from peers. Upon winning, the node transitions to <em>LEADER</em>
 *       and begins sending <em>heartbeats</em> (AppendEntries with empty log entries) to maintain leadership.</li>
 *   <li><em>Log Replication</em>:
 *       The leader appends commands to its log and sends <code>APPEND_ENTRIES</code> messages to all followers.
 *       Followers validate <code>prevLogIndex</code> and <code>prevLogTerm</code> to maintain a consistent log.
 *       When a majority acknowledges an entry, the leader <em>commits</em> it and notifies others.</li>
 *   <li><em>Fault Tolerance</em>:
 *       Followers track the leader’s <code>commitIndex</code> and apply any newly committed entries to their local state.
 *       If the leader fails or becomes unreachable, a follower times out and starts a new election by incrementing
 *       its term and sending <code>REQUEST_VOTE</code> messages to other servers.</li>
 * </ul>
 * <p>
 * <strong>Key Design Decisions:</strong>
 * <ul>
 *   <li>We store log entries in an in-memory <code>List&lt;LogEntry&gt;</code> (<code>log</code> field),
 *       indexed from 0. A production system would require durable storage (disk, etc.).</li>
 *   <li>When we become leader, we set each follower’s <code>nextIndexMap</code> to
 *       <code>lastLogIndex + 1</code>, forcing them to catch up from the end of the leader's log.</li>
 *   <li>We do not implement advanced features like log compaction or snapshotting in this simplified version.</li>
 *   <li>No built-in reconfiguration mechanism. The set of nodes is assumed fixed during runtime.</li>
 * </ul>
 * <p>
 * <strong>Limitations:</strong>
 * <ul>
 *   <li>No robust election timeout: if multiple nodes start elections at once, there could be repeated term increments
 *       before a stable leader emerges. A future solution would randomize timeouts to reduce collisions.</li>
 *   <li>Network partitions are minimally handled. If a majority partition does not include the leader,
 *       that majority will eventually elect a new leader. The old leader in the minority partition will stop committing logs
 *       once it sees higher terms in <code>APPEND_ENTRIES_RESPONSE</code> or <code>REQUEST_VOTE_RESPONSE</code>.</li>
 *   <li>Durability is not guaranteed. We do not persist <code>currentTerm</code> or the log to stable storage,
 *       meaning a node restart would lose its state.</li>
 * </ul>
 *
 * <p>This class extends {@link com.dss.backend.consensus.AbstractConsensusAlgorithm} to inherit default
 * no-op implementations of some methods (e.g., <code>accept()</code>) which are not used in Raft.</p>
 *
 * @author Daniel Morales
 */
public class Raft extends AbstractConsensusAlgorithm {

    private final AppLogger appLogger = new DefaultAppLogger(Raft.class);

    /**
     * Role of this node in the Raft cluster:
     * <ul>
     *   <li><code>FOLLOWER</code>: responds to RPCs from candidates and leaders.</li>
     *   <li><code>CANDIDATE</code>: issues <code>REQUEST_VOTE</code> to become leader.</li>
     *   <li><code>LEADER</code>: the main replication source, sending <code>APPEND_ENTRIES</code> to followers.</li>
     * </ul>
     */
    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    // ----------------------------------------------------
    // Persistent State on All Servers
    // (In real Raft, these would be persisted to stable storage.)
    // ----------------------------------------------------
    private volatile int currentTerm = 0;    // Latest term server has seen
    private volatile String votedFor = null; // Candidate ID that received vote in current term
    private final List<LogEntry> log = new ArrayList<>(); // The Raft log

    // ----------------------------------------------------
    // Volatile State on All Servers
    // ----------------------------------------------------
    private volatile int commitIndex = 0;    // Index of highest log entry known to be committed
    private volatile int lastApplied = 0;    // Index of highest log entry applied to state machine

    // ----------------------------------------------------
    // Volatile State on Leaders (Reinitialized after election)
    // ----------------------------------------------------
    private final Map<String, Integer> nextIndexMap = new ConcurrentHashMap<>();
    private final Map<String, Integer> matchIndexMap = new ConcurrentHashMap<>();

    // Node identification
    private final String myNodeId;
    private final List<String> allNodeIds;
    private final MessageRouter router;

    // Election-related state
    private volatile Role role = Role.FOLLOWER;
    private final Map<Integer, Integer> votesReceivedPerTerm = new ConcurrentHashMap<>();

    private final ConsensusBroadcaster broadcaster;

    /**
     * Constructor for Raft, initializing the node’s ID, known node list,
     * and the shared {@link MessageRouter}.
     *
     * @param nodeId     the ID of this node
     * @param allNodeIds a list of IDs for all nodes in the cluster
     * @param router     the message router used to send/receive {@link SimulationMessage}s
     */
    public Raft(String nodeId, List<String> allNodeIds, MessageRouter router) {
        this.myNodeId = nodeId;
        this.allNodeIds = allNodeIds;
        this.router = router;
        this.broadcaster = new ConsensusBroadcaster(router, myNodeId);
    }

    /**
     * Proposes a new command to the Raft cluster. Only the leader appends the new entry to its log
     * and replicates it to followers via <code>APPEND_ENTRIES</code>.
     * <p>
     * If this node is not the leader, we simply log a message and do nothing.
     *
     * @param value the command or value to be appended to the log
     */
    @Override
    public void propose(Object value) {
        if (role != Role.LEADER) {
            appLogger.info("Raft Node {} is not LEADER; ignoring propose() or forwarding to leader.", myNodeId);
            return;
        }
        // Append the new command to the leader’s log
        LogEntry entry = new LogEntry(currentTerm, value);
        log.add(entry);
        int newEntryIndex = log.size() - 1;
        appLogger.info("Leader {} appended new command at index={} for term={}", myNodeId, newEntryIndex, currentTerm);

        // Leader is always up-to-date with itself
        matchIndexMap.put(myNodeId, newEntryIndex);

        // Send AppendEntries (heartbeats or new entries) to all followers
        broadcastAppendEntries();
    }

    /**
     * Raft does not use a separate <code>accept()</code> method directly; it is handled
     * by the <code>APPEND_ENTRIES</code> handler in {@link #handleMessage(SimulationMessage)}.
     */

    /**
     * Commits a command once it reaches the commit index. This node’s local state machine
     * would apply the command here (in a real system).
     *
     * @param value the command/value being committed
     */
    @Override
    public void commit(Object value) {
        appLogger.info("Raft Node {} commits: {}", myNodeId, value);
        // Application-specific logic would apply the command to a state machine here
    }

    /**
     * The central dispatch method for all incoming Raft-related messages.
     *
     * @param msg the incoming message (e.g., <code>REQUEST_VOTE</code>, <code>APPEND_ENTRIES</code>)
     */
    @Override
    public void handleMessage(SimulationMessage msg) {
        RaftPayload payload = ConsensusUtils.safeCastPayload(msg, RaftPayload.class);
        if (payload == null) {
            return;
        }
        switch (payload.getType()) {
            case REQUEST_VOTE:
                handleRequestVote(msg.getSourceNodeId(), payload);
                break;
            case REQUEST_VOTE_RESPONSE:
                handleRequestVoteResponse(msg.getSourceNodeId(), payload);
                break;
            case APPEND_ENTRIES:
                handleAppendEntries(msg.getSourceNodeId(), payload);
                break;
            case APPEND_ENTRIES_RESPONSE:
                handleAppendEntriesResponse(msg.getSourceNodeId(), payload);
                break;
            default:
                appLogger.debug("Raft: Unhandled message type: {}", payload.getType());
                break;
        }
    }

    // ----------------------------------------------------------------
    //                     ELECTION METHODS
    // ----------------------------------------------------------------

    /**
     * Initiates a new election by becoming a candidate, incrementing the term,
     * and sending <code>REQUEST_VOTE</code> to peers.
     */
    public void triggerElection() {
        becomeCandidate();
        requestVotesFromPeers();
    }

    /**
     * Transitions this node to the <code>CANDIDATE</code> role:
     * increments <code>currentTerm</code>, votes for itself, and logs the transition.
     */
    private void becomeCandidate() {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor = myNodeId;
        votesReceivedPerTerm.put(currentTerm, 1); // Vote for self
        appLogger.info("{} becomes CANDIDATE for term {}", myNodeId, currentTerm);
    }

    /**
     * Broadcasts <code>REQUEST_VOTE</code> to all nodes, asking them to vote for this candidate.
     */
    private void requestVotesFromPeers() {
        RaftPayload voteRequest = new RaftPayload();
        voteRequest.setType(MessageType.REQUEST_VOTE);
        voteRequest.setTerm(currentTerm);
        voteRequest.setCandidateId(myNodeId);

        broadcaster.broadcast(MessageType.REQUEST_VOTE, voteRequest, ProtocolType.RAFT);
    }

    /**
     * Handles an incoming <code>REQUEST_VOTE</code> message.
     * <p>
     * If the candidate’s term is higher, become follower. If the term matches this node’s
     * and we have not yet voted or have previously voted for this candidate, grant the vote.
     *
     * @param sourceNode the candidate requesting the vote
     * @param rp         the RaftPayload containing term and candidate ID
     */
    private void handleRequestVote(String sourceNode, RaftPayload rp) {
        int term = rp.getTerm();
        String candidateId = rp.getCandidateId();

        if (term > currentTerm) {
            becomeFollower(term);
        }

        boolean grantVote = false;
        // If we are in the same term and haven’t voted or have voted for this candidate
        if (term == currentTerm && (votedFor == null || votedFor.equals(candidateId))) {
            grantVote = true;
            votedFor = candidateId;
        }

        // Send REQUEST_VOTE_RESPONSE
        RaftPayload response = new RaftPayload();
        response.setType(MessageType.REQUEST_VOTE_RESPONSE);
        response.setTerm(currentTerm);
        response.setVoteGranted(grantVote);

        SimulationMessage sm = SimulationMessageFactory.createMessage(
                myNodeId, sourceNode, MessageType.REQUEST_VOTE_RESPONSE, response, ProtocolType.RAFT);
        router.messageSent(sm);
    }

    /**
     * Handles an incoming <code>REQUEST_VOTE_RESPONSE</code>.
     * <p>
     * If <code>voteGranted = true</code> and we are still in <code>CANDIDATE</code> role for that term,
     * tally the vote. If we have a majority, become leader.
     *
     * @param sourceNode the node that voted or rejected
     * @param rp         the RaftPayload with <code>term</code> and <code>voteGranted</code>
     */
    private void handleRequestVoteResponse(String sourceNode, RaftPayload rp) {
        int term = rp.getTerm();
        boolean voteGranted = rp.isVoteGranted();

        if (term > currentTerm) {
            // A higher term encountered means we revert to follower
            becomeFollower(term);
            return;
        }

        // If we are still a candidate in the same term and got a “yes” vote, increment
        if (role == Role.CANDIDATE && term == currentTerm && voteGranted) {
            int voteCount = votesReceivedPerTerm.getOrDefault(currentTerm, 0) + 1;
            votesReceivedPerTerm.put(currentTerm, voteCount);
            // Once we have a majority, become leader
            if (voteCount >= ((allNodeIds.size() / 2) + 1)) {
                becomeLeader();
            }
        }
    }

    /**
     * Reverts this node to FOLLOWER role, setting a new <code>currentTerm</code>
     * and clearing our <code>votedFor</code>.
     *
     * @param newTerm the new term (larger than our currentTerm)
     */
    private void becomeFollower(int newTerm) {
        appLogger.info("{} becomes FOLLOWER in term {}", myNodeId, newTerm);
        role = Role.FOLLOWER;
        currentTerm = newTerm;
        votedFor = null;
    }

    /**
     * Promotes this node to LEADER role once it obtains a majority of votes.
     * <p>
     * Initializes <code>nextIndexMap</code> and <code>matchIndexMap</code> for replication
     * and immediately sends out <em>heartbeats</em> (empty <code>APPEND_ENTRIES</code>) to followers.
     */
    private void becomeLeader() {
        role = Role.LEADER;
        appLogger.info("{} is now LEADER in term {}", myNodeId, currentTerm);

        int lastLogIndex = log.size() - 1;
        for (String nodeId : allNodeIds) {
            nextIndexMap.put(nodeId, lastLogIndex + 1);
            matchIndexMap.put(nodeId, -1);
        }
        // The leader obviously is caught up to its own log
        matchIndexMap.put(myNodeId, lastLogIndex);

        // Send heartbeats (AppendEntries) to all followers
        broadcastAppendEntries();
    }

    // ----------------------------------------------------------------
    //                   LOG REPLICATION METHODS
    // ----------------------------------------------------------------

    /**
     * Broadcasts <code>APPEND_ENTRIES</code> (heartbeat or actual entries) to all followers.
     * If this node is not the leader, does nothing.
     */
    private void broadcastAppendEntries() {
        if (role != Role.LEADER) return;

        for (String follower : allNodeIds) {
            if (follower.equals(myNodeId)) continue;
            sendAppendEntriesTo(follower);
        }
    }

    /**
     * Sends <code>APPEND_ENTRIES</code> to a specific follower, containing any log entries
     * the follower has not yet received.
     *
     * @param follower the follower’s node ID
     */
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

        SimulationMessage sm = SimulationMessageFactory.createMessage(
                myNodeId, follower, MessageType.APPEND_ENTRIES, payload, ProtocolType.RAFT);
        router.messageSent(sm);
    }

    /**
     * Handles incoming <code>APPEND_ENTRIES</code> requests from a leader.
     * <p>
     * If the leader’s term is higher than ours, become follower.
     * Validate the <code>prevLogIndex</code> and <code>prevLogTerm</code>.
     * If they match, append any new entries. Then advance <code>commitIndex</code> if the leader’s
     * commit is higher.
     *
     * @param sourceNode the leader sending the append
     * @param rp         the RaftPayload with log entries, prevLogIndex/Term, etc.
     */
    private void handleAppendEntries(String sourceNode, RaftPayload rp) {
        int leaderTerm = rp.getTerm();

        // If we see a higher term, become follower
        if (leaderTerm > currentTerm) {
            becomeFollower(leaderTerm);
        }
        // If the leader’s term is still lower, reject the append
        if (leaderTerm < currentTerm) {
            sendAppendEntriesResponse(false, log.size(), -1, sourceNode);
            return;
        }

        // If we are a candidate or leader at the same term, convert to follower
        if (role != Role.FOLLOWER) {
            role = Role.FOLLOWER;
        }

        int prevLogIndex = rp.getPrevLogIndex();
        int prevLogTerm = rp.getPrevLogTerm();

        // Validate log matching
        if (prevLogIndex >= 0) {
            if (prevLogIndex >= log.size() ||
                    log.get(prevLogIndex).getTerm() != prevLogTerm) {
                int matchIdx = Math.min(prevLogIndex, log.size());
                sendAppendEntriesResponse(false, matchIdx,
                        (prevLogIndex < log.size()) ? log.get(prevLogIndex).getTerm() : -1,
                        sourceNode);
                return;
            }
        }

        // If valid, append any new entries
        List<LogEntry> entries = rp.getEntries();
        int currentIndex = prevLogIndex + 1;
        for (LogEntry newEntry : entries) {
            if (currentIndex < log.size()) {
                // If we find a conflicting entry with a different term, delete the existing
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

        // Update commitIndex if leader’s commit is greater
        if (rp.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(rp.getLeaderCommit(), log.size() - 1);
            applyEntries();
        }

        int lastAppended = currentIndex - 1;
        sendAppendEntriesResponse(true, lastAppended, -1, sourceNode);
    }

    /**
     * Applies any newly committed entries from the log to this node’s local state.
     * <p>
     * In practice, this might forward commands to a state machine. Here, we simply log them.
     */
    private void applyEntries() {
        while (lastApplied < commitIndex + 1) {
            LogEntry entry = log.get(lastApplied);
            lastApplied++;
            commit(entry.getCommand());
        }
    }

    /**
     * Constructs and sends an <code>APPEND_ENTRIES_RESPONSE</code> to the leader, indicating
     * whether the append was successful or not.
     *
     * @param success      true if the <code>prevLogIndex/prevLogTerm</code> check passed
     * @param matchIndex   the highest log index matched so far
     * @param conflictTerm the term of the conflicting entry, if any
     * @param targetNode   the leader to send the response to
     */
    private void sendAppendEntriesResponse(boolean success, int matchIndex, int conflictTerm, String targetNode) {
        RaftPayload rp = new RaftPayload();
        rp.setType(MessageType.APPEND_ENTRIES_RESPONSE);
        rp.setTerm(currentTerm);
        rp.setSuccess(success);
        rp.setMatchIndex(matchIndex);
        rp.setConflictTerm(conflictTerm);

        SimulationMessage sm = SimulationMessageFactory.createMessage(
                myNodeId, targetNode, MessageType.APPEND_ENTRIES_RESPONSE, rp, ProtocolType.RAFT);
        router.messageSent(sm);
    }

    /**
     * Handles <code>APPEND_ENTRIES_RESPONSE</code> on the leader. If the append fails due to a log mismatch,
     * decrement <code>nextIndex</code> and retry. If it succeeds, update <code>matchIndexMap</code>
     * and advance the leader’s <code>commitIndex</code> if a majority of matchIndexes are high enough.
     *
     * @param follower the responding follower
     * @param rp       the RaftPayload with <code>success</code>, <code>matchIndex</code>, etc.
     */
    private void handleAppendEntriesResponse(String follower, RaftPayload rp) {
        if (role != Role.LEADER) return;

        if (rp.getTerm() > currentTerm) {
            // Step down if we see a higher term
            becomeFollower(rp.getTerm());
            return;
        }

        boolean success = rp.isSuccess();
        if (!success) {
            // Mismatch: decrement nextIndex and retry
            int fallbackIndex = rp.getMatchIndex();
            int oldNext = nextIndexMap.get(follower);
            int newNext = Math.min(oldNext - 1, fallbackIndex);
            newNext = Math.max(newNext, 0);
            nextIndexMap.put(follower, newNext);
            sendAppendEntriesTo(follower);
        } else {
            // Update matchIndex / nextIndex
            int matchIndex = rp.getMatchIndex();
            matchIndexMap.put(follower, matchIndex);
            nextIndexMap.put(follower, matchIndex + 1);

            // Check if we can advance commitIndex
            for (int i = log.size() - 1; i > commitIndex; i--) {
                int replicatedCount = 1; // Count the leader itself
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