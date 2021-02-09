package com.baichen.jraft.log;

import com.baichen.jraft.Lifecycle;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.util.Subscription;

import java.util.Collection;
import java.util.function.Consumer;

public interface LogBatcher extends Lifecycle {

    void add(LogEntry log);

    void add(Collection<LogEntry> logs);

    void flush();

    Subscription subscribe(Consumer<Collection<LogEntry>> sub);

}

