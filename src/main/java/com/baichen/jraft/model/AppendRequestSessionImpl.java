package com.baichen.jraft.model;

import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.value.LogEntryId;
import com.baichen.jraft.value.AppendRequestSessionState;
import com.google.common.util.concurrent.SettableFuture;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AppendRequestSessionImpl implements AppendRequestSession {

    private long id;

    private LogEntry log;

    private AppendRequestSessionState state;

    private SettableFuture<AppendRequestSession> future;

    private final byte[] command;

    public AppendRequestSessionImpl(long id,byte[] command) {
        this.id = id;
        this.command= command;
        this.state = AppendRequestSessionState.CREATED;
        this.future = SettableFuture.create();
    }

    @Override
    public byte[] getCommand() {
        return command;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public LogEntry getLog() {
        return log;
    }

    @Override
    public void setLog(LogEntry log) {
        this.log = log;
    }

    @Override
    public AppendRequestSessionState getState() {
        return state;
    }


    @Override
    public void complete() {

        if (future != null) {
            this.future.set(this);

        }

    }

    @Override
    public void cancel() {
        if (future != null && !future.isCancelled()) {
            future.cancel(true);
        }
    }

    @Override
    public void await() throws ExecutionException, InterruptedException {
        if (future != null) {
            future.get();

        }
    }

    @Override
    public void await(long time, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (future != null) {
            future.get(time, unit);
        }
    }


}
