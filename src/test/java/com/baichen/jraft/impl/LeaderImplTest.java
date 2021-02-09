package com.baichen.jraft.impl;

import com.baichen.jraft.HeartbeatTimer;
import com.baichen.jraft.RequestIdGenerator;
import com.baichen.jraft.log.LogStream;
import com.baichen.jraft.log.factory.LogFactory;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.membership.Peer;
import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.model.AppendRequestSession;
import com.baichen.jraft.model.VolatileState;
import com.baichen.jraft.transport.RpcResult;
import com.baichen.jraft.value.NodeInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class LeaderImplTest extends AbstractServerStateInternalTest {


    private VolatileState volatileState;

    private LeaderImpl leader;

    @Mock
    private RequestIdGenerator requestIdGenerator;

    @Mock
    private Peer peer1;

    @Mock
    private Peer peer2;

    @Mock
    private HeartbeatTimer heartbeatTimer;

    @Mock
    private LogStream logStream;

    @Mock
    private LogFactory logFactory;


    @Before
    public void init() {
        super.init();


        stateStorage.updateTerm(1);
        logStorage.appendLog(new LogEntry(1, 10));

        volatileState = new VolatileState(10, 10);

        when(logFactory.createLogStream(any(Peer.class))).thenReturn(logStream);

        leader = new LeaderImpl(server, () -> logManager, peer -> logFactory.createLogStream(peer), stateStorage, volatileState, requestIdGenerator);
        when(requestIdGenerator.next()).thenReturn(1);
        when(server.getPeers()).thenReturn(Arrays.asList(peer1, peer2));

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


    }

    @Test
    public void testAppend_noPeers() {
        when(server.getPeers()).thenReturn(Collections.emptyList());
        leader.start();

        AppendRequestSession session = leader.createAppendSession(new byte[0]);
        leader.append(session);
        Assert.assertEquals(11, volatileState.getCommitIndex());


    }

    @Test
    public void testStart() {

        leader.start();
        ArgumentCaptor<AppendRequest> argument = ArgumentCaptor.forClass(AppendRequest.class);
        verify(logStream, times(2)).append(argument.capture());

        AppendRequest value = argument.getValue();
        Assert.assertEquals(logStorage.getLastLogIndex(), value.getPrevLogIndex());
        Assert.assertEquals(logStorage.getLastLogTerm(), value.getPrevLogTerm());
        Assert.assertEquals(stateStorage.getState().getTerm(), value.getTerm());
        Assert.assertEquals(volatileState.getCommitIndex(), value.getLeaderCommit());
        Assert.assertEquals(nodeInfo.getId(), value.getLeaderId());


    }

    @Test
    public void testOnAppendReplyReceived_ack() throws InterruptedException {
        leader.start();

        LogEntry log = new LogEntry(1, 11);
        logStorage.appendLog(log);

        RpcResult reply = new RpcResult();
        reply.setTerm(1);
        reply.setNodeId(peer1.getNodeInfo().getId());
        reply.setCommitIndex(11);

        leader.onAppendReplyReceived(reply);
        Assert.assertEquals(11, volatileState.getCommitIndex());


    }


    /**
     * leader starts at index 10, sends out 1011 logs
     * peer1 replies 101 accepted
     * peer2 replies 100 accepted
     * peer3 replies 100 accepted
     * peer4 replies 101 accepted
     *
     * @throws InterruptedException
     */
    @Test
    public void testOnAppendReplyReceived_outOfOrderReply() throws InterruptedException {

        Peer peer3 = Mockito.mock(Peer.class);

        NodeInfo nodeInfo3 = new NodeInfo();
        nodeInfo3.setId("peer3");
        nodeInfo3.setHost("127.0.0.1");
        nodeInfo3.setPort(1003);
        nodeInfo3.setName("Peer 3");
        when(peer3.getNodeInfo()).thenReturn(nodeInfo3);

        Peer peer4 = Mockito.mock(Peer.class);

        NodeInfo nodeInfo4 = new NodeInfo();
        nodeInfo4.setId("peer4");
        nodeInfo4.setHost("127.0.0.1");
        nodeInfo4.setPort(1004);
        nodeInfo4.setName("Peer 4");
        when(peer4.getNodeInfo()).thenReturn(nodeInfo4);

        when(server.getPeers()).thenReturn(Arrays.asList(peer1, peer2, peer3, peer4));
        leader.start();


        for (int i = 11; i < 1011; i++) {
            LogEntry log = new LogEntry(1, i);
            logStorage.appendLog(log);
        }

        RpcResult reply = new RpcResult();
        reply.setTerm(1);
        reply.setNodeId(peer1.getNodeInfo().getId());
        reply.setCommitIndex(101);

        leader.onAppendReplyReceived(reply);

        Assert.assertEquals(10, volatileState.getCommitIndex());

        reply = new RpcResult();
        reply.setTerm(1);
        reply.setNodeId(peer2.getNodeInfo().getId());
        reply.setCommitIndex(100);

        leader.onAppendReplyReceived(reply);
        Assert.assertEquals(100, volatileState.getCommitIndex());


        reply = new RpcResult();
        reply.setTerm(1);
        reply.setNodeId(peer3.getNodeInfo().getId());
        reply.setCommitIndex(100);

        leader.onAppendReplyReceived(reply);
        Assert.assertEquals(100, volatileState.getCommitIndex());

        reply = new RpcResult();
        reply.setTerm(1);
        reply.setNodeId(peer4.getNodeInfo().getId());
        reply.setCommitIndex(101);
        leader.onAppendReplyReceived(reply);

        Assert.assertEquals(101, volatileState.getCommitIndex());


    }


    /**
     * leader peer1 starts at index 10, sends out 1011 logs
     * peer1 has just started, at index 0, rejected
     * peer2 replies 11 accepted
     * peer3 replies 12 accepted
     * peer4 replies 100 accepted
     *
     * @throws InterruptedException
     */
    @Test
    public void testOnAppendReplyReceived_someRejectedDueToInconsistency() throws InterruptedException {

        Peer peer3 = Mockito.mock(Peer.class);

        NodeInfo nodeInfo3 = new NodeInfo();
        nodeInfo3.setId("peer3");
        nodeInfo3.setHost("127.0.0.1");
        nodeInfo3.setPort(1003);
        nodeInfo3.setName("Peer 3");
        when(peer3.getNodeInfo()).thenReturn(nodeInfo3);

        Peer peer4 = Mockito.mock(Peer.class);

        NodeInfo nodeInfo4 = new NodeInfo();
        nodeInfo4.setId("peer4");
        nodeInfo4.setHost("127.0.0.1");
        nodeInfo4.setPort(1004);
        nodeInfo4.setName("Peer 4");
        when(peer4.getNodeInfo()).thenReturn(nodeInfo4);

        when(server.getPeers()).thenReturn(Arrays.asList(peer1, peer2, peer3, peer4));
        leader.start();


        for (int i = 11; i < 1011; i++) {
            LogEntry log = new LogEntry(1, i);
            logStorage.appendLog(log);
        }

        RpcResult reply = new RpcResult();
        reply.setTerm(1);
        reply.setNodeId(peer1.getNodeInfo().getId());
        reply.setCode(RpcResult.Codes.LOG_CONFLICT);
        reply.setCommitIndex(0);

        leader.onAppendReplyReceived(reply);

        Assert.assertEquals(10, volatileState.getCommitIndex());

        reply = new RpcResult();
        reply.setTerm(1);
        reply.setNodeId(peer2.getNodeInfo().getId());
        reply.setCommitIndex(100);

        leader.onAppendReplyReceived(reply);
        Assert.assertEquals(10, volatileState.getCommitIndex());


        reply = new RpcResult();
        reply.setTerm(1);
        reply.setNodeId(peer3.getNodeInfo().getId());
        reply.setCommitIndex(11);
        leader.onAppendReplyReceived(reply);

        Assert.assertEquals(11, volatileState.getCommitIndex());


        reply = new RpcResult();
        reply.setTerm(1);
        reply.setNodeId(peer4.getNodeInfo().getId());
        reply.setCommitIndex(100);
        leader.onAppendReplyReceived(reply);

        Assert.assertEquals(100, volatileState.getCommitIndex());

    }

}
