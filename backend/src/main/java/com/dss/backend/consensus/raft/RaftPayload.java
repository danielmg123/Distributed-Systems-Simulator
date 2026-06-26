package com.dss.backend.consensus.raft;

import com.dss.backend.messaging.MessageType;
import lombok.Data;

import java.util.List;

/**
 * Holds the data exchanged in Raft protocol messages, including AppendEntries and RequestVote.
 * <p>
 * Raft relies on these fields to perform leader election, log replication, and to track commit indices.
 * Most fields map directly to the AppendEntries or RequestVote RPC parameters.
 */
@Data
public class RaftPayload {

    /**
     * The high-level message type (e.g., REQUEST_VOTE, APPEND_ENTRIES, etc.).
     */
    private MessageType type;

    /**
     * The sender's current term.
     * <ul>
     *   <li>Leaders include their term in AppendEntries.</li>
     *   <li>Candidates increment their term upon starting an election.</li>
     * </ul>
     */
    private int term;

    /**
     * Used in REQUEST_VOTE messages to indicate which node is the candidate.
     */
    private String candidateId;

    /**
     * Used in REQUEST_VOTE messages so the voter can apply Raft's election
     * restriction: a candidate may only receive a vote if its log is at least
     * as up to date as the voter's own log. This is the index of the
     * candidate's last log entry (-1 if its log is empty).
     */
    private int lastLogIndex;

    /**
     * Used alongside {@link #lastLogIndex} in REQUEST_VOTE messages: the term
     * of the candidate's last log entry (-1 if its log is empty).
     */
    private int lastLogTerm;

    /**
     * Used in APPEND_ENTRIES messages to indicate the current leader’s ID.
     */
    private String leaderId;

    /**
     * Log entries to store (send in AppendEntries).
     * May be empty for heartbeats (i.e., no new log entries).
     */
    private List<LogEntry> entries;

    /**
     * Index of log entry immediately preceding new entries.
     * This is how the leader ensures that a follower’s log matches before it appends more entries.
     */
    private int prevLogIndex;

    /**
     * Term of prevLogIndex entry.
     * If a follower’s log at prevLogIndex does not have this term, it refuses the AppendEntries.
     */
    private int prevLogTerm;

    /**
     * Leader’s commitIndex, telling followers the highest log entry known to be committed.
     */
    private int leaderCommit;

    /**
     * Used in APPEND_ENTRIES_RESPONSE to indicate success or failure of the append attempt.
     */
    private boolean success;

    /**
     * Used in REQUEST_VOTE_RESPONSE to indicate whether this node granted its vote to the candidate.
     */
    private boolean voteGranted;

    /**
     * In AppendEntries responses, the index of the last entry that matched
     * or the fallback index to try next time if there was a mismatch.
     */
    private int matchIndex;

    /**
     * In conflict scenarios, the follower can provide an index to help
     * the leader reduce the backtracking required to find the conflict.
     */
    private int conflictIndex;

    /**
     * The term of the conflicting entry, used by the leader to skip
     * all entries in the same conflicting term.
     */
    private int conflictTerm;
}