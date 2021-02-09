package com.baichen.jraft.impl;

import com.baichen.jraft.ElectionTimer;
import com.baichen.jraft.Follower;
import com.baichen.jraft.Server;
import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.Replicator;
import com.baichen.jraft.log.impl.ReplicatorImpl;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.meta.PersistentState;
import com.baichen.jraft.meta.StateStorage;
import com.baichen.jraft.model.AppendReply;
import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.model.VolatileState;
import com.baichen.jraft.model.VoteRequest;
import com.baichen.jraft.transport.RpcResult;
import com.baichen.jraft.value.ServerState;
import org.apache.logging.log4j.Logger;

public class FollowerImpl implements Follower {

    private static final Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(FollowerImpl.class);

    private LogStorage logStorage;

    private StateStorage stateStorage;

    private VolatileState volatileState;

    private Server server;

    private Replicator replicator;

    private ElectionTimer electionTimer;

    public FollowerImpl(Server server, LogStorage logStorage,
                        StateStorage stateStorage, VolatileState volatileState, ElectionTimer electionTimer) {
        this.server = server;
        this.logStorage = logStorage;
        this.stateStorage = stateStorage;
        this.volatileState = volatileState;
        this.replicator = new ReplicatorImpl(server, logStorage, stateStorage, volatileState);
        this.electionTimer = electionTimer;
    }


    private void handleElectionTimeout() {
        server.transitionState(ServerState.CANDIDATE);
    }


    @Override
    public void start() {
        replicator.start();
        electionTimer.subscribeTimeout(this::handleElectionTimeout);
        electionTimer.start();

    }


    @Override
    public void destroy() {
        replicator.destroy();
        electionTimer.destroy();
        logStorage = null;
        server = null;
    }

    @Override
    public RpcResult handleRequestVoteReceived(VoteRequest request) {

        //only vote for candidates with more up-to-date logs
        PersistentState state = stateStorage.getState();
        int currentTerm = state.getTerm();

        boolean granted = request.getTerm() >= currentTerm;

        //if candidate's term is older than this server, reject
        if (!granted) {

            RpcResult result = new RpcResult();
            result.setNodeId(server.getNodeInfo().getId());
            result.setCode(RpcResult.Codes.VOTE_REJECT);
            result.setTerm(currentTerm);
            result.setMsg("Received older term");
            result.setRequestId(request.getRequestId());
            return result;
        }

        if (request.getTerm() > currentTerm) {
            stateStorage.updateTerm(request.getTerm());
            currentTerm = request.getTerm();
        }

        String votedFor = state.getVotedFor();

        granted = votedFor == null || state.getVotedForTerm() < state.getTerm();
        //if this server has voted for the currentTerm, reject
        if (!granted) {
            RpcResult result = new RpcResult();
            result.setNodeId(server.getNodeInfo().getId());
            result.setCode(RpcResult.Codes.VOTE_REJECT);
            result.setTerm(currentTerm);
            result.setMsg("Has voted for current term");
            result.setRequestId(request.getRequestId());
            return result;
        }

        granted = request.getLastLogIndex() >= logStorage.getLastLogIndex();
        //candidate’s log is at least
        //as up-to-date as this server’s log, grant vote
        if (granted) {
            stateStorage.updateVoteFor(request.getCandidateId(), state.getTerm());
            RpcResult result = new RpcResult();
            result.setNodeId(server.getNodeInfo().getId());
            result.setRequestId(request.getRequestId());
            result.setTerm(currentTerm);
            return result;
        } else {
            RpcResult result = new RpcResult();
            result.setNodeId(server.getNodeInfo().getId());
            result.setRequestId(request.getRequestId());
            result.setTerm(currentTerm);
            result.setCode(RpcResult.Codes.VOTE_REJECT_STALE_LOG);
            result.setMsg("Candidate log is not up to date");
            return result;

        }

    }

    @Override
    public RpcResult handleAppendReceived(AppendRequest request) {

        LOGGER.debug("{} Receive AppendRequest, requestId: {}, prevLogTerm: {},  prevLogIndex: {}, logs: {}",
                server.getNodeInfo().getId(),
                request.getRequestId(),
                request.getPrevLogTerm(), request.getPrevLogIndex(), request.getLogs() == null ? 0 : request.getLogs().size());
        electionTimer.reset();
        PersistentState state = stateStorage.getState();

        int currentTerm = state.getTerm();

        if (request.getTerm() > currentTerm) {
            stateStorage.updateTerm(request.getTerm());
        }

        AppendReply appendReply = replicator.receiveAppend(request);

        RpcResult result = new RpcResult();
        if (appendReply.isInconsistent()) {
            result.setCode(RpcResult.Codes.LOG_CONFLICT);
        }
        result.setNodeId(server.getNodeInfo().getId());
        result.setTerm(appendReply.getTerm());
        result.setCommitIndex(appendReply.getCommitIndex());
        result.setRequestId(request.getRequestId());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} Respond AppendRequest, requestId: {}, code: {}, term: {}, commitIndex: {}",
                    server.getNodeInfo().getId(),
                    result.getRequestId(), result.getCode(), result.getTerm(), result.getCommitIndex());
        }

        return result;

    }

    @Override
    public LogEntry getLastLog() {
        return logStorage.getLastLog();
    }

    @Override
    public int getLastLogIndex() {
        return logStorage.getLastLogIndex();
    }

    @Override
    public int getLastLogTerm() {
        return logStorage.getLastLogTerm();
    }
}
