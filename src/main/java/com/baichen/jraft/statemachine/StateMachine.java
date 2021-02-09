package com.baichen.jraft.statemachine;

import com.baichen.jraft.Lifecycle;
import com.baichen.jraft.log.model.LogEntry;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;

public interface StateMachine extends Lifecycle {

    ListenableFuture<Void> apply(LogEntry log);

    ListenableFuture<Void> apply(Collection<LogEntry> logs);


}
