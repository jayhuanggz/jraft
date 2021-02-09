package com.baichen.jraft.log.impl;

import com.baichen.jraft.Server;
import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.Replicator;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.meta.PersistentState;
import com.baichen.jraft.meta.StateStorage;
import com.baichen.jraft.model.AppendReply;
import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.model.VolatileState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ReplicatorImpl implements Replicator {

    private Server server;

    private LogStorage logStorage;

    private StateStorage stateStorage;

    private VolatileState volatileState;

    public ReplicatorImpl(Server server, LogStorage logStorage,
                          StateStorage stateStorage, VolatileState volatileState) {
        this.server = server;
        this.logStorage = logStorage;
        this.stateStorage = stateStorage;
        this.volatileState = volatileState;
    }

    @Override
    public AppendReply receiveAppend(AppendRequest request) {

        PersistentState state = stateStorage.getState();

        int currentTerm = state.getTerm();

        int prevLogTerm = request.getPrevLogTerm();
        int prevLogIndex = request.getPrevLogIndex();

        if (prevLogIndex > 0) {
            LogEntry log = logStorage.findByIndex(prevLogIndex);

            if (log == null || log.getTerm() != prevLogTerm) {

                AppendReply reply = new AppendReply(server.getNodeInfo().getId(), currentTerm, false, volatileState.getCommitIndex());
                reply.setInconsistent(true);
                return reply;
            }
            List<LogEntry> newLogs = request.getLogs();
            if (newLogs != null && !newLogs.isEmpty()) {

                int conflictStartPosition = -1;
                List<LogEntry> logsToAppend = new ArrayList<>(newLogs.size());

                for (int i = 0; i < newLogs.size(); i++) {
                    LogEntry newLog = newLogs.get(i);
                    LogEntry existingLog = logStorage.findByIndex(newLog.getIndex());
                    if (existingLog != null && existingLog.getTerm() != newLog.getTerm()) {
                        //if new log conflicts with the existing log,
                        //delete the existing entry and all that
                        //follow it
                        logStorage.deleteLogsStartingFrom(newLog.getIndex());
                        conflictStartPosition = i;
                        break;
                    } else if (existingLog == null) {
                        logsToAppend.add(newLog);
                    }
                }

                if (conflictStartPosition >= 0) {
                    for (int i = conflictStartPosition; i < newLogs.size(); i++) {
                        logsToAppend.add(newLogs.get(i));
                    }
                }

                if (!logsToAppend.isEmpty()) {
                    logStorage.batchAppend(logsToAppend);

                }
            }

        } else {
            logStorage.batchAppend(request.getLogs());


        }

        if (request.getLeaderCommit() > volatileState.getCommitIndex()) {
            volatileState.setCommitIndex(Math.min(request.getLeaderCommit(), logStorage.getLastLogIndex()));
        } else {
            volatileState.setCommitIndex(logStorage.getLastLogIndex());
        }
        return new AppendReply(server.getNodeInfo().getId(), currentTerm,
                true, volatileState.getCommitIndex());

    }

    @Override
    public void start() {

    }

    @Override
    public void destroy() {

        logStorage = null;
        stateStorage = null;
        server = null;
        volatileState = null;

    }
}
