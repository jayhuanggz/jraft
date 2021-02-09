package com.baichen.jraft.impl;

import com.baichen.jraft.ElectionTimer;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.value.LogEntryId;
import com.baichen.jraft.meta.PersistentStateImpl;
import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.model.VolatileState;
import com.baichen.jraft.model.VoteRequest;
import com.baichen.jraft.transport.RpcResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.UUID;

@RunWith(MockitoJUnitRunner.class)
public class FollowerImplTest extends AbstractServerStateInternalTest {

    private PersistentStateImpl persistentState = new PersistentStateImpl();

    private VolatileState volatileState;

    private FollowerImpl follower;

    private LogEntryId logId;

    private LogEntry log;

    private String votedForId = UUID.randomUUID().toString();

    @Mock
    private ElectionTimer electionTimer;

    @Before
    public void init() {
        super.init();

        logId = new LogEntryId(5, 10);
        log = new LogEntry(logId.getTerm(), logId.getIndex(), new byte[10]);

        persistentState.setTerm(5);
        persistentState.setVotedFor(null);

        stateStorage.updateVoteFor(persistentState.getVotedFor(), log.getTerm());
        stateStorage.updateTerm(persistentState.getTerm());
        logStorage.appendLog(log);


        volatileState = new VolatileState(0, 0);
        follower = new FollowerImpl(server, logStorage, stateStorage, volatileState, electionTimer);
        follower.start();
    }

    @Test
    public void testOnRequestVoteReceived_currentTermIsNewer() throws InterruptedException {

        VoteRequest request = new VoteRequest(1, 4, UUID.randomUUID().toString(), 10, 4);

        RpcResult result = follower.handleRequestVoteReceived(request);
        Assert.assertEquals(RpcResult.Codes.VOTE_REJECT, result.getCode());
        Assert.assertEquals(stateStorage.getState().getTerm(), result.getTerm());

    }


    @Test
    public void testOnRequestVoteReceived_alreadyVotedForCurrentTerm() throws InterruptedException {
        stateStorage.updateVoteFor(votedForId, 6);

        VoteRequest request = new VoteRequest(1, 6, UUID.randomUUID().toString(), 10, 6);

        RpcResult result = follower.handleRequestVoteReceived(request);

        Assert.assertEquals(RpcResult.Codes.VOTE_REJECT, result.getCode());
    }


    @Test
    public void testOnRequestVoteReceived_CandidateLogIsNotUpToDate() throws InterruptedException {

        VoteRequest request = new VoteRequest(1, 5, UUID.randomUUID().toString(), 9, 5);

        RpcResult result = follower.handleRequestVoteReceived(request);

        Assert.assertEquals(RpcResult.Codes.REJECT, result.getCode());

    }

    @Test
    public void testOnRequestVoteReceived_candidateLogIsNewer() throws InterruptedException {

        String candidateId = UUID.randomUUID().toString();
        VoteRequest request = new VoteRequest(1, 5, candidateId, 11, 5);

        RpcResult result = follower.handleRequestVoteReceived(request);

        Assert.assertEquals(RpcResult.Codes.OK, result.getCode());
        Assert.assertEquals(candidateId, stateStorage.getState().getVotedFor());

    }

    @Test
    public void testReceiveAppendWhenNoLog() {

        logStorage.deleteLogsStartingFrom(1);
        stateStorage.updateTerm(0);
        LogEntry newLog = new LogEntry(1, 1, new byte[0]);

        Assert.assertEquals(0, stateStorage.getState().getTerm());
        Assert.assertNull(logManager.findByIndex(1));

        AppendRequest request = AppendRequest.append(1, 1, UUID.randomUUID().toString(), 0, 0, 0, newLog);
        RpcResult result = follower.handleAppendReceived(request);
        Assert.assertEquals(RpcResult.Codes.OK, result.getCode());
        Assert.assertEquals(1, stateStorage.getState().getTerm());
        Assert.assertNotNull(logManager.findByIndex(1));
        Assert.assertEquals(1, volatileState.getCommitIndex());


    }

    @Test
    public void testReceiveMultipleAppends() {

        logStorage.deleteLogsStartingFrom(1);
        stateStorage.updateTerm(0);


        AppendRequest request1 = AppendRequest.append(1, 1, UUID.randomUUID().toString(), 0, 0, 0, new LogEntry(1, 1, new byte[0]));
        follower.handleAppendReceived(request1);
        Assert.assertEquals(1, volatileState.getCommitIndex());

        AppendRequest request2 = AppendRequest.append(2, 1, UUID.randomUUID().toString(), 0, 0, 0, new LogEntry(1, 2, new byte[0]));
        follower.handleAppendReceived(request2);
        Assert.assertEquals(2, volatileState.getCommitIndex());

        AppendRequest request3 = AppendRequest.append(3, 1, UUID.randomUUID().toString(), 0, 0, 0, new LogEntry(1, 3, new byte[0]));
        follower.handleAppendReceived(request3);
        Assert.assertEquals(3, volatileState.getCommitIndex());


        Assert.assertNotNull(logManager.findByIndex(1));
        Assert.assertNotNull(logManager.findByIndex(2));
        Assert.assertNotNull(logManager.findByIndex(3));

    }

    @Test
    public void testOnAppendReceived_updateTermIfNewer() throws InterruptedException {

        Assert.assertEquals(5, stateStorage.getState().getTerm());
        AppendRequest request = AppendRequest.heartbeat(1, 6, UUID.randomUUID().toString(), 10, 5, 10);
        follower.handleAppendReceived(request);
        Assert.assertEquals(6, stateStorage.getState().getTerm());
        Assert.assertEquals(10, volatileState.getCommitIndex());
    }


}
