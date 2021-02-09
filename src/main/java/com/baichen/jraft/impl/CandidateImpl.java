package com.baichen.jraft.impl;

import com.baichen.jraft.Candidate;
import com.baichen.jraft.ElectionTimer;
import com.baichen.jraft.RequestIdGenerator;
import com.baichen.jraft.Server;
import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.Replicator;
import com.baichen.jraft.log.impl.ReplicatorImpl;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.meta.PersistentState;
import com.baichen.jraft.meta.StateStorage;
import com.baichen.jraft.model.Election;
import com.baichen.jraft.model.ElectionImpl;
import com.baichen.jraft.model.VolatileState;
import com.baichen.jraft.value.ServerState;
import org.apache.logging.log4j.Logger;

public class CandidateImpl implements Candidate {

    private static final Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(CandidateImpl.class);

    private Server server;

    private LogStorage logStorage;

    private StateStorage stateStorage;

    private VolatileState volatileState;

    private Election election;

    private Replicator replicator;

    private RequestIdGenerator requestIdGenerator;

    private ElectionTimer electionTimer;

    public CandidateImpl(Server server, LogStorage logStorage,
                         StateStorage stateStorage, VolatileState volatileState,
                         RequestIdGenerator requestIdGenerator, ElectionTimer electionTimer) {
        this.server = server;
        this.stateStorage = stateStorage;
        this.volatileState = volatileState;
        this.replicator = new ReplicatorImpl(server, logStorage, stateStorage, volatileState);
        this.requestIdGenerator = requestIdGenerator;
        this.electionTimer = electionTimer;
        this.logStorage = logStorage;
    }


    private void handleElectionTimeout() {
        if (election.wins()) {
            LOGGER.info("Election wins for term {}, will become Leader......", election.getTerm());
            server.transitionState(ServerState.LEADER);
        } else {
            LOGGER.info("Election times out for term {}, starting a new one", election.getTerm());
            election.destroy();
            startNewElection();
        }
    }


    private void startNewElection() {
        PersistentState state = stateStorage.getState();
        int term = state.getTerm();
        term++;
        stateStorage.updateTerm(term);
        this.election = new ElectionImpl(server, logStorage, requestIdGenerator, term, server.getMajorityCount());
        election.start();
        LOGGER.info("Started new election for term {}", election.getTerm());
        electionTimer.reset();

    }

    @Override
    public void start() {

        if (server.getPeers().isEmpty()) {
            stateStorage.incrementTerm();
            server.transitionState(ServerState.LEADER);
        } else {
            replicator.start();
            electionTimer.subscribeTimeout(this::handleElectionTimeout);
            electionTimer.start();

            startNewElection();

        }
    }

    @Override
    public void destroy() {
        logStorage = null;
        if (election != null) {
            election.destroy();
        }
        electionTimer.destroy();
        replicator.destroy();
        server = null;


    }

    @Override
    public void handleVoteReplyFromPeer(String nodeId) {
        election.receiveVoteReply(nodeId);
        if (election.wins()) {
            server.transitionState(ServerState.LEADER);
        }
    }

    public Election getElection() {
        return election;
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
