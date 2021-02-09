package com.baichen.jraft.impl;

import com.baichen.jraft.*;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.value.LogEntryId;
import com.baichen.jraft.membership.Peer;
import com.baichen.jraft.model.VolatileState;
import com.baichen.jraft.value.NodeInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class CandidateImplTest extends AbstractServerStateInternalTest {

    private VolatileState volatileState;

    private CandidateImpl candidate;

    private LogEntryId logId;

    private LogEntry log;

    @Mock
    private ElectionTimer electionTimer;

    @Mock
    private Peer peer1;

    @Mock
    private Peer peer2;

    private RequestIdGenerator requestIdGenerator;


    @Before
    public void init() {
        super.init();

        logId = new LogEntryId(5, 10);
        log = new LogEntry(logId.getTerm(), logId.getIndex(), new byte[10]);


        stateStorage.updateTerm(5);
        logStorage.appendLog(log);

        requestIdGenerator = new AtomicRequestIdGenerator();


        volatileState = new VolatileState(0, 0);

        NodeInfo nodeInfo1 = new NodeInfo();
        nodeInfo1.setId("peer1");
        nodeInfo1.setHost("127.0.0.1");
        nodeInfo1.setPort(1001);
        nodeInfo1.setName("Peer 1");
        when(peer1.getNodeInfo()).thenReturn(nodeInfo1);

        NodeInfo nodeInfo2 = new NodeInfo();
        nodeInfo2.setId("peer2");
        nodeInfo2.setHost("127.0.0.1");
        nodeInfo2.setPort(1002);
        nodeInfo2.setName("Peer 2");
        when(peer2.getNodeInfo()).thenReturn(nodeInfo2);

        when(server.getPeers()).thenReturn(Arrays.asList(peer1, peer2));
        when(server.getMajorityCount()).thenReturn(1);


        candidate = new CandidateImpl(server, logStorage, stateStorage, volatileState, requestIdGenerator, electionTimer);
        candidate.start();
    }


    @Test
    public void testHandleVoteReplyFromPeer() {

        candidate.handleVoteReplyFromPeer(peer1.getNodeInfo().getId());
        Assert.assertEquals(1, candidate.getElection().getGrantedCount());

    }

    @Test
    public void testHandleVoteReplyFromPeer_duplicateVote() {

        candidate.handleVoteReplyFromPeer(peer1.getNodeInfo().getId());
        candidate.handleVoteReplyFromPeer(peer1.getNodeInfo().getId());
        Assert.assertEquals(1, candidate.getElection().getGrantedCount());

    }

    @Test
    public void testHandleVoteReplyFromPeer_wins() {

        candidate.handleVoteReplyFromPeer(peer1.getNodeInfo().getId());
        candidate.handleVoteReplyFromPeer(peer2.getNodeInfo().getId());
        Assert.assertEquals(2, candidate.getElection().getGrantedCount());
        Assert.assertTrue(candidate.getElection().wins());
    }
}
