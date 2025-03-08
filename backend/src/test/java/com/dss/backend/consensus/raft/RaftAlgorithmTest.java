package com.dss.backend.consensus.raft;

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
        raft = new Raft("node1", Arrays.asList("node1", "node2", "node3"), mockRouter);
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

        // Use reflection to verify that the Raft log now contains the new entry.
        try {
            java.lang.reflect.Field logField = Raft.class.getDeclaredField("log");
            logField.setAccessible(true);
            Object logObj = logField.get(raft);
            assertTrue(logObj instanceof java.util.List);
            java.util.List<?> logList = (java.util.List<?>) logObj;
            assertEquals(1, logList.size());
            LogEntry entry = (LogEntry) logList.get(0);
            assertEquals("command1", entry.getCommand());
        } catch (Exception e) {
            fail("Reflection failed: " + e.getMessage());
        }
    }

    @Test
    public void becomeLeader_SetsRoleAndBroadcastsHeartbeats() {
        // Invoke the private becomeLeader() method via reflection.
        try {
            java.lang.reflect.Method becomeLeader = Raft.class.getDeclaredMethod("becomeLeader");
            becomeLeader.setAccessible(true);
            becomeLeader.invoke(raft);

            // Verify that the role has been updated to LEADER.
            java.lang.reflect.Field roleField = Raft.class.getDeclaredField("role");
            roleField.setAccessible(true);
            Object role = roleField.get(raft);
            assertEquals(Raft.Role.LEADER, role);

            // Verify that the router was used to send at least one APPEND_ENTRIES message (heartbeats).
            ArgumentCaptor<SimulationMessage> captor = ArgumentCaptor.forClass(SimulationMessage.class);
            verify(mockRouter, atLeastOnce()).messageSent(captor.capture());
            boolean heartbeatFound = captor.getAllValues().stream()
                    .anyMatch(msg -> msg.getType() == MessageType.APPEND_ENTRIES);
            assertTrue(heartbeatFound, "Leader should broadcast APPEND_ENTRIES as heartbeats.");
        } catch (Exception e) {
            fail("Reflection failed: " + e.getMessage());
        }
    }
}