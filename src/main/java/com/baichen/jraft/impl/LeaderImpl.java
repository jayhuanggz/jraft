package com.baichen.jraft.impl;

import com.baichen.jraft.*;
import com.baichen.jraft.log.LogManager;
import com.baichen.jraft.log.LogStream;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.value.LogEntryId;
import com.baichen.jraft.membership.Peer;
import com.baichen.jraft.meta.PersistentState;
import com.baichen.jraft.meta.StateStorage;
import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.model.AppendRequestSession;
import com.baichen.jraft.model.VolatileState;
import com.baichen.jraft.transport.RpcResult;
import com.baichen.jraft.value.NodeInfo;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class LeaderImpl implements Leader {

    private static final Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(LeaderImpl.class);


    private Server server;

    private LogManager logManager;

    private StateStorage stateStorage;

    private VolatileState volatileState;

    private Map<String, Integer> nextIndex = new HashMap<>();

    private Map<String, Integer> matchIndex = new HashMap<>();

    private RequestIdGenerator requestIdGenerator;


    private Function<Peer, LogStream> logStreamFactory;

    private Map<String, LogStream> logStreams = new HashMap<>();

    private AppendRequestSessionManager appendSessionManager;

    public LeaderImpl(Server server,
                      Supplier<LogManager> logManagerFactory,
                      Function<Peer, LogStream> logStreamFactory,
                      StateStorage stateStorage,
                      VolatileState volatileState,
                      RequestIdGenerator requestIdGenerator) {
        this.server = server;
        this.logManager = logManagerFactory.get();
        this.stateStorage = stateStorage;
        this.volatileState = volatileState;
        this.requestIdGenerator = requestIdGenerator;
        this.logStreamFactory = logStreamFactory;
        this.appendSessionManager = new AppendRequestSessionManagerImpl();


    }


    @Override
    public void onAppendReplyReceived(RpcResult reply) {

        String nodeId = reply.getNodeId();
        PersistentState state = stateStorage.getState();

        int newNextIndex = nextIndex.get(nodeId);

        if (reply.getCode() == 0) {
            // upon receiving a success append reply(ack),
            // update nextIndex and matchIndex for the follower,
            // then do the majority check
            int commitIndex = reply.getCommitIndex();
            matchIndex.put(nodeId, commitIndex);
            newNextIndex = commitIndex + 1;
            nextIndex.put(nodeId, newNextIndex);
            checkCommit(commitIndex);


        } else if (reply.getCode() == RpcResult.Codes.LOG_CONFLICT) {
            // if failed because of log inconsistency,
            // decrement nextIndex and retry

            newNextIndex = Math.max(1, newNextIndex - 1);
            nextIndex.put(nodeId, newNextIndex);

            if (newNextIndex <= logManager.getLastLogIndex()) {
                for (Peer peer : server.getPeers()) {
                    if (peer.getNodeInfo().getId().equals(nodeId)) {
                        sendAppendToPeer(peer, state, newNextIndex);
                        break;
                    }
                }
            }
        }


    }

    private void checkCommit(int matchIndex) {

        int majority = server.getPeers().size() / 2;
        int mod = server.getPeers().size() % 2;
        if (mod > 0) {
            majority++;
        }
        int commitIndex = volatileState.getCommitIndex();

        if (commitIndex >= matchIndex) {
            return;
        }

        LogEntry log = logManager.findByIndex(matchIndex);
        if (log == null) {
            return;
        }

        if (log.getTerm() != stateStorage.getState().getTerm()) {
            return;
        }

        int count = 0;
        for (Map.Entry<String, Integer> entry : this.matchIndex.entrySet()) {
            if (entry.getValue() >= matchIndex) {
                if (++count >= majority) {
                    volatileState.setCommitIndex(matchIndex);
                    onCommitIndexChanged(matchIndex);
                    break;

                }
            }
        }
    }

    private void onCommitIndexChanged(int newCommitIndex) {
        appendSessionManager.commitSession(new LogEntryId(stateStorage.getState().getTerm(), newCommitIndex));
    }

    private void sendAppendToPeer(Peer peer, PersistentState state, int nextIndex) {

        int lastLogIndex = logManager.getLastLogIndex();

        if (lastLogIndex < nextIndex) {
            //it is heartbeat
            AppendRequest request = AppendRequest.heartbeat(requestIdGenerator.next(), state.getTerm(),
                    server.getNodeInfo().getId(), lastLogIndex, logManager.getLastLogTerm(),
                    volatileState.getCommitIndex());
            appendLogToStream(peer, request);
            return;
        }

        List<LogEntry> logs = logManager.findByIndexStartingFrom(nextIndex);

        int prevLogIndex = Math.max(0, nextIndex - 1);
        int prevLogTerm = 0;

        if (prevLogIndex > 0) {
            LogEntry log = logManager.findByIndex(prevLogIndex);
            if (log != null) {
                prevLogTerm = log.getTerm();
            }
        }


        AppendRequest request = AppendRequest.append(requestIdGenerator.next(), state.getTerm(),
                server.getNodeInfo().getId(), prevLogIndex, prevLogTerm,
                volatileState.getCommitIndex(), logs);

        appendLogToStream(peer, request);

    }

    private void appendLogToStream(Peer peer, AppendRequest request) {


        NodeInfo nodeInfo = peer.getNodeInfo();

        LogStream logStream = logStreams.get(nodeInfo.getId());
        assert logStream != null;

        LOGGER.debug("Sending AppendRequest to peer {}@{}:{}, term: {}, prevLogTerm: {}, prevLogIndex: {} ",
                nodeInfo.getId(), nodeInfo.getHost(), nodeInfo.getPort(), request.getTerm(),
                request.getPrevLogTerm(), request.getPrevLogIndex());
        boolean sent = logStream.append(request);
        if (!sent) {
            LOGGER.info("Failed to send AppendRequest to peer {}@{}:{}, term: {}, prevLogTerm: {}, prevLogIndex: {} ",
                    nodeInfo.getId(), nodeInfo.getHost(), nodeInfo.getPort(), request.getTerm(),
                    request.getPrevLogTerm(), request.getPrevLogIndex());
        }

    }

    public void sendHeartBeat() {

        PersistentState state = stateStorage.getState();

        for (Peer peer : server.getPeers()) {
            sendAppendToPeer(peer, state, logManager.getLastLogIndex() + 1);
        }
    }


    @Override
    public void append(AppendRequestSession session) {

        PersistentState state = stateStorage.getState();

        LogEntry log = new LogEntry(state.getTerm(), logManager.getLastLogIndex() + 1, session.getCommand());

        logManager.append(log);


        appendSessionManager.startSession(session, log, 300000, new AppendRequestSessionListener() {
            @Override
            public void onTimeout(AppendRequestSession session) {

            }

            @Override
            public void onCommitted(AppendRequestSession session) {

            }

            @Override
            public void onApplied(AppendRequestSession session) {

            }
        });

        if (server.getPeers().isEmpty()) {
            volatileState.setCommitIndex(logManager.getLastLogIndex());
            appendSessionManager.commitSession(new LogEntryId(log.getTerm(), log.getIndex()));
        } else {

            for (Peer peer : server.getPeers()) {
                sendAppendToPeer(peer, stateStorage.getState(), log.getIndex());
            }
        }
    }

    @Override
    public LogEntry getLastLog() {
        return logManager.getLastLog();
    }

    @Override
    public int getLastLogIndex() {
        return logManager.getLastLogIndex();
    }

    @Override
    public int getLastLogTerm() {
        return logManager.getLastLogTerm();
    }
    @Override
    public AppendRequestSession createAppendSession(byte[] command) {
        return appendSessionManager.createSession(command);
    }

    @Override
    public void start() {
        appendSessionManager.start();
        logManager.start();
        int lastLogIndex = logManager.getLastLogIndex();

        volatileState.setCommitIndex(lastLogIndex);

        for (Peer peer : server.getPeers()) {
            nextIndex.put(peer.getNodeInfo().getId(), lastLogIndex + 1);
            LogStream logStream = logStreamFactory.apply(peer);
            logStreams.put(peer.getNodeInfo().getId(), logStream);
            logStream.start();
        }


        sendHeartBeat();

    }

    @Override
    public void destroy() {


        appendSessionManager.destroy();
        server = null;
        logStreamFactory = null;
        logManager.destroy();
        for (LogStream logStream : logStreams.values()) {
            logStream.destroy();
        }
        logStreams.clear();


    }
}
