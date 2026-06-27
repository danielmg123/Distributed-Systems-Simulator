package com.dss.backend.consensus.raft;

import com.dss.backend.config.SimulationProperties;
import com.dss.backend.engine.Scheduler;
import com.dss.backend.messaging.MessageRouter;
import com.dss.backend.messaging.MessageType;
import com.dss.backend.messaging.ProtocolType;
import com.dss.backend.messaging.SimulationMessage;
import com.dss.backend.messaging.SimulationMessageFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class RaftAlgorithmTest {

    private MessageRouter mockRouter;
    private Raft raft;

    @BeforeEach
    public void setUp() {
        mockRouter = mock(MessageRouter.class);
        when(mockRouter.getRegisteredNodeIds()).thenReturn(Set.of("node1", "node2", "node3"));
        Scheduler mockScheduler = mock(Scheduler.class);
        raft = new Raft("node1", Arrays.asList("node1", "node2", "node3"), mockRouter,
                mockScheduler, new SimulationProperties());
    }

    @Test
    public void handleMessage_RequestVote_GrantVoteIfConditionsMet() {
        // Create a REQUEST_VOTE message from candidate "node2" for term 1.
        RaftPayload voteRequest = new RaftPayload();
        voteRequest.setType(MessageType.REQUEST_VOTE);
        voteRequest.setTerm(1);
        voteRequest.setCandidateId("node2");

        SimulationMessage requestMsg = SimulationMessageFactory.createMessage("node2", "node1", MessageType.REQUEST_VOTE, voteRequest, ProtocolType.RAFT);
        raft.handleMessage(requestMsg);

        // Capture the REQUEST_VOTE_RESPONSE message.
        ArgumentCaptor<SimulationMessage> captor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeastOnce()).messageSent(captor.capture());
        SimulationMessage responseMsg = captor.getValue();
        assertEquals(MessageType.REQUEST_VOTE_RESPONSE, responseMsg.getType());
        RaftPayload responsePayload = (RaftPayload) responseMsg.getPayload();
        assertEquals(1, responsePayload.getTerm());
        assertTrue(responsePayload.isVoteGranted());
    }

    @Test
    public void handleMessage_AppendEntries_UpdatesLog() {
        // Simulate an APPEND_ENTRIES message with a new log entry.
        LogEntry newEntry = new LogEntry(1, "command1");
        RaftPayload appendPayload = new RaftPayload();
        appendPayload.setType(MessageType.APPEND_ENTRIES);
        appendPayload.setTerm(1);
        appendPayload.setLeaderId("node2");
        appendPayload.setPrevLogIndex(-1);
        appendPayload.setPrevLogTerm(-1);
        appendPayload.setEntries(Arrays.asList(newEntry));
        appendPayload.setLeaderCommit(0);

        SimulationMessage appendMsg = SimulationMessageFactory.createMessage("node2", "node1", MessageType.APPEND_ENTRIES, appendPayload, ProtocolType.RAFT);
        raft.handleMessage(appendMsg);

        assertEquals(1, raft.getLog().size());
        assertEquals("command1", raft.getLog().get(0).getCommand());
    }

    @Test
    public void handleMessage_RequestVote_DeniedWhenCandidateLogIsLessUpToDate() throws Exception {
        // Seed the voter's own log with an entry from a higher term than the candidate
        // claims to have. Even though the REQUEST_VOTE's term matches our currentTerm
        // (so the term check alone would grant it), Raft's election restriction
        // (Section 5.4.1) requires the candidate's log to be at least as up to date as
        // ours. A candidate with a lower-term last log entry must be denied, otherwise
        // it could win the election and overwrite already-committed entries.
        java.lang.reflect.Field logField = Raft.class.getDeclaredField("log");
        logField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.List<LogEntry> voterLog = (java.util.List<LogEntry>) logField.get(raft);
        voterLog.add(new LogEntry(5, "voter-command")); // voter's last log term = 5

        RaftPayload voteRequest = new RaftPayload();
        voteRequest.setType(MessageType.REQUEST_VOTE);
        voteRequest.setTerm(0); // matches the voter's default currentTerm (0)
        voteRequest.setCandidateId("node2");
        voteRequest.setLastLogIndex(0);
        voteRequest.setLastLogTerm(2); // candidate's last log term is lower than the voter's (5)

        SimulationMessage requestMsg = SimulationMessageFactory.createMessage(
                "node2", "node1", MessageType.REQUEST_VOTE, voteRequest, ProtocolType.RAFT);
        raft.handleMessage(requestMsg);

        ArgumentCaptor<SimulationMessage> captor = ArgumentCaptor.forClass(SimulationMessage.class);
        verify(mockRouter, atLeastOnce()).messageSent(captor.capture());
        SimulationMessage responseMsg = captor.getValue();
        assertEquals(MessageType.REQUEST_VOTE_RESPONSE, responseMsg.getType());
        RaftPayload responsePayload = (RaftPayload) responseMsg.getPayload();
        assertFalse(responsePayload.isVoteGranted(),
                "Vote must be denied when the candidate's log is less up to date, even if the term matches.");
    }

    // becomeLeader_SetsRoleAndBroadcastsHeartbeats was removed: it drove leadership by
    // reflecting into the private becomeLeader() method. Now that Phase 3 makes
    // triggerElection() reachable via a real (randomized) election timer,
    // RaftClusterIntegrationTest's "boots leaderless" scenario covers the same
    // behavior through the real public path and the new getRole()/getLog() accessors,
    // with actual majority votes from other real nodes instead of a single mocked one.

    @Test
    public void getRoleLabel_ReflectsCurrentRole() {
        assertEquals("FOLLOWER", raft.getRoleLabel(), "A freshly-constructed node starts as FOLLOWER");
    }
}