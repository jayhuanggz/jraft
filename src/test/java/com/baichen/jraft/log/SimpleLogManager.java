package com.baichen.jraft.log;

import com.baichen.jraft.log.model.LogEntry;

import java.util.List;

public class SimpleLogManager implements LogManager {

    private LogStorage logStorage;

    public SimpleLogManager(LogStorage logStorage) {
        this.logStorage = logStorage;
    }

    @Override
    public void append(LogEntry log) {
        logStorage.appendLog(log);
    }

    @Override
    public void append(List<LogEntry> logs) {
        logStorage.batchAppend(logs);
    }

    @Override
    public LogEntry findByIndex(int index) {
        return logStorage.findByIndex(index);
    }

    @Override
    public List<LogEntry> findByIndexStartingFrom(int startIndex) {
        return logStorage.findByIndexStartingFrom(startIndex);
    }

    @Override
    public int getLastLogTerm() {
        return logStorage.getLastLogTerm();
    }

    @Override
    public int getLastLogIndex() {
        return logStorage.getLastLogIndex();
    }

    @Override
    public LogEntry getLastLog() {
        return logStorage.getLastLog();
    }

    @Override
    public void start() {

    }

    @Override
    public void destroy() {

    }
}
