package com.baichen.jraft.log;

import com.baichen.jraft.log.impl.AbstractLogBatcher;
import com.baichen.jraft.log.model.LogEntry;

import java.util.ArrayList;
import java.util.List;

public class SimpleLogBatcher extends AbstractLogBatcher {

    private List<LogEntry> logs;

    private int batchSize;

    public SimpleLogBatcher(int batchSize) {
        logs = new ArrayList<>(batchSize);
        this.batchSize = batchSize;
    }

    @Override
    protected List<LogEntry> getBatch() {
        return logs;
    }

    @Override
    protected boolean shouldFlush() {
        return logs.size() >= batchSize;
    }

    @Override
    protected void cleanUp() {

    }
}
