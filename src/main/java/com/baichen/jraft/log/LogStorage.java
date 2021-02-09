package com.baichen.jraft.log;

import com.baichen.jraft.Lifecycle;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.value.LogEntryId;

import java.util.List;

public interface LogStorage extends Lifecycle {

    void appendLog(LogEntry log);

    void batchAppend(List<LogEntry> logs);

    LogEntry getLastLog();

    LogEntry findByIndex(int index);

    int deleteLogsStartingFrom(int startIndex);

    List<LogEntry> findByIndexStartingFrom(int startIndex);

    List<LogEntry> findByIndexInRange(int startIndex, int endIndex);

    int getLastLogTerm();

    int getLastLogIndex();


}
