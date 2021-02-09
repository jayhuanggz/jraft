package com.baichen.jraft.log.impl;

import com.baichen.jraft.Server;
import com.baichen.jraft.log.LogManager;
import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.Replicator;
import com.baichen.jraft.log.SimpleLogManager;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.value.LogEntryId;
import com.baichen.jraft.meta.PersistentStateImpl;
import com.baichen.jraft.meta.StateStorage;
import com.baichen.jraft.model.AppendReply;
import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.model.VolatileState;
import com.baichen.jraft.test.InMemoryLogStorage;
import com.baichen.jraft.test.InMemoryStateStorage;
import com.baichen.jraft.value.NodeInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.UUID;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReplicatorImplTest {

    @Mock
    private Server server;

    private LogStorage logStorage;

    private StateStorage stateStorage;

    private NodeInfo nodeInfo;

    private PersistentStateImpl persistentState = new PersistentStateImpl();

    private VolatileState volatileState;

    private Replicator replicator;

    private LogManager logManager;

    @Before
    public void init() {
        MockitoAnnotations.openMocks(ReplicatorImplTest.class);

        nodeInfo = new NodeInfo();
        nodeInfo.setId("node1");
        nodeInfo.setHost("127.0.0.1");
        nodeInfo.setPort(1000);
        nodeInfo.setName("Node 1");
        when(server.getNodeInfo()).thenReturn(nodeInfo);
        logStorage = new InMemoryLogStorage();
        stateStorage = new InMemoryStateStorage();
        for (int i = 1; i <= 10; i++) {
            LogEntryId logId = new LogEntryId(5, i);
            LogEntry log = new LogEntry(logId.getTerm(), logId.getIndex());
            logStorage.appendLog(log);
        }

        persistentState.setTerm(5);
        persistentState.setVotedFor(null);

        stateStorage.updateVoteFor(persistentState.getVotedFor(), 5);
        stateStorage.updateTerm(persistentState.getTerm());

        logManager = new SimpleLogManager(logStorage);
        volatileState = new VolatileState(0, 0);
        replicator = new ReplicatorImpl(server, logStorage, stateStorage, volatileState);
        replicator.start();
    }

    @Test
    public void testReceiveAppend_matchLastLogWithLowerLeaderCommit() throws InterruptedException {
        //index 10 is matched, but leader commit is 5
        AppendRequest request = AppendRequest.heartbeat(1, 5, UUID.randomUUID().toString(), 10, 5, 5);
        AppendReply reply = replicator.receiveAppend(request);
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(5, volatileState.getCommitIndex());
    }

    @Test
    public void testReceiveAppend_matchLastLogWithLatestLeaderCommit() throws InterruptedException {
        //index 10 is matched, and leader commit is 10 which matches the last log index
        AppendRequest request = AppendRequest.heartbeat(1, 5, UUID.randomUUID().toString(), 10, 5, 10);
        AppendReply reply = replicator.receiveAppend(request);
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(10, volatileState.getCommitIndex());
    }

    @Test
    public void testReceiveAppend_matchLastLogWithHigherLeaderCommit() throws InterruptedException {
        //index 10 is matched, and leader commit is 20 which is higher than the last log index
        AppendRequest request = AppendRequest.heartbeat(1, 5, UUID.randomUUID().toString(), 10, 5, 20);
        AppendReply reply = replicator.receiveAppend(request);

        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(10, volatileState.getCommitIndex());
    }


    @Test
    public void testReceiveAppend_matchOlderLog() throws InterruptedException {
        //index 4 is matched
        AppendRequest request = AppendRequest.heartbeat(1, 5, UUID.randomUUID().toString(), 4, 5, 5);
        AppendReply reply = replicator.receiveAppend(request);
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(5, volatileState.getCommitIndex());
    }


    @Test
    public void testReceiveAppend_logNotFound() throws InterruptedException {
        AppendRequest request = AppendRequest.heartbeat(1, 5, UUID.randomUUID().toString(), 11, 5, 10);
        AppendReply reply = replicator.receiveAppend(request);
        Assert.assertFalse(reply.isSuccess());

    }

    @Test
    public void testReceivedAppend_LogConflicts() throws InterruptedException {

        LogEntry log1 = new LogEntry(5, 11);
        LogEntry log2 = new LogEntry(5, 12);
        LogEntry log3 = new LogEntry(5, 13);

        logStorage.appendLog(log1);
        logStorage.appendLog(log2);
        logStorage.appendLog(log3);

        Assert.assertEquals(13, logStorage.getLastLogIndex());

        //newer term to make a conflict
        LogEntry requestLog = new LogEntry(6, 11);

        AppendRequest request = AppendRequest.append(1, 6, UUID.randomUUID().toString(), 10, 5, 10, requestLog);

        AppendReply reply = replicator.receiveAppend(request);
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(11, logStorage.getLastLogIndex());
        Assert.assertEquals(6, logStorage.getLastLogTerm());

        Assert.assertEquals(10, volatileState.getCommitIndex());
    }


}
