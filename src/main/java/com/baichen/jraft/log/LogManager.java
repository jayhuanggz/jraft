package com.baichen.jraft.log;

import com.baichen.jraft.Lifecycle;
import com.baichen.jraft.log.model.LogEntry;

import java.util.List;

public interface LogManager extends Lifecycle {

    void append(LogEntry log);

    void append(List<LogEntry> logs);

    LogEntry findByIndex(int index);

    List<LogEntry> findByIndexStartingFrom(int startIndex);

    int getLastLogTerm();

    int getLastLogIndex();

    LogEntry getLastLog();


}
