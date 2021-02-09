package com.baichen.jraft.model;

import com.baichen.jraft.RequestIdGenerator;
import com.baichen.jraft.Server;
import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.membership.Peer;

import java.util.HashSet;
import java.util.Set;

public class ElectionImpl implements Election {

    private Server server;

    private LogStorage logStorage;

    private int term;

    private int minVote;

    private boolean win;

    private int grantedCount;

    private RequestIdGenerator requestIdGenerator;

    private Set<String> grantedNodes = new HashSet<>();

    public ElectionImpl(Server server, LogStorage logStorage,
                        RequestIdGenerator requestIdGenerator,
                        int term, int minVote) {
        this.server = server;
        this.term = term;
        this.minVote = minVote;
        this.logStorage = logStorage;
        this.requestIdGenerator = requestIdGenerator;
    }


    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public boolean wins() {
        return win;
    }

    @Override
    public void receiveVoteReply(String nodeId) {

        if (grantedNodes.add(nodeId)) {
            if (++grantedCount >= minVote) {
                this.win = true;
            }
        }


    }

    @Override
    public int getGrantedCount() {
        return grantedCount;
    }

    @Override
    public void start() {
        for (Peer peer : server.getPeers()) {
            peer.requestVote(new VoteRequest(requestIdGenerator.next(), term, peer.getNodeInfo().getId(), logStorage.getLastLogIndex(), logStorage.getLastLogTerm()));
        }
    }

    @Override
    public void destroy() {
        server = null;
        logStorage = null;
        requestIdGenerator = null;

    }
}
