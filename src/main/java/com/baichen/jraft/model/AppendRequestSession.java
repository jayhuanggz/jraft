package com.baichen.jraft.model;

import com.baichen.jraft.Lifecycle;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.value.LogEntryId;
import com.baichen.jraft.value.AppendRequestSessionState;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Represents an append request session. It is initialized when
 * the leader receives an AppendRequest and exists until the log is
 * committed by majority and applied to state machine
 */
public interface AppendRequestSession {

    long getId();

    byte[] getCommand();

    LogEntry getLog();

    void setLog(LogEntry log);

    AppendRequestSessionState getState();

    void complete();

    void cancel();

    void await() throws ExecutionException, InterruptedException;

    void await(long time, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;

}
