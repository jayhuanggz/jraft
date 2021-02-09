package com.baichen.jraft.log;

import com.baichen.jraft.Lifecycle;
import com.baichen.jraft.log.model.LogEntry;

import java.util.Collection;

public interface LogCache extends Lifecycle {

    void append(LogEntry log);

    void append(Collection<LogEntry> logs);

    LogEntry getFirstLog();

    LogEntry getLastLog();

    LogEntry findByIndex(int index);

    int deleteLogsStartingFrom(int startIndex);

    int deleteLogsBefore(int endIndex);

    LogEntry[] findByIndexStartingFrom(int startIndex);

    int size();


}
