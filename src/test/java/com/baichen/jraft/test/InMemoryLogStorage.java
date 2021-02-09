package com.baichen.jraft.test;

import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.model.LogEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryLogStorage implements LogStorage {

    private List<LogEntry> logs = new LinkedList<>();

    private volatile LogEntry lastLog;

    private void onLogsChanged() {
        if (logs.isEmpty()) {
            lastLog = null;
        } else {
            lastLog = logs.get(logs.size() - 1);
        }
    }

    @Override
    public synchronized void appendLog(LogEntry log) {
        logs.add(log);
        onLogsChanged();
    }

    @Override
    public synchronized void batchAppend(List<LogEntry> logs) {
        this.logs.addAll(logs);
        onLogsChanged();

    }

    @Override
    public synchronized LogEntry getLastLog() {
        return lastLog;
    }


    @Override
    public synchronized LogEntry findByIndex(int index) {
        for (LogEntry log : logs) {

            if (index == log.getIndex()) {
                return log;
            }
        }
        return null;
    }

    @Override
    public synchronized int deleteLogsStartingFrom(int startIndex) {

        AtomicInteger result = new AtomicInteger(0);
        logs.removeIf(log -> {
            boolean match = log.getIndex() >= startIndex;

            if (match) {
                result.incrementAndGet();
            }
            return match;
        });
        onLogsChanged();
        return result.get();
    }

    @Override
    public synchronized List<LogEntry> findByIndexStartingFrom(int startIndex) {

        int position = -1;
        for (int i = 0; i < logs.size(); i++) {
            if (logs.get(i).getIndex() == startIndex) {
                position = i;
                break;

            }
        }

        if (position == -1) {
            return Collections.emptyList();
        }


        return new ArrayList<>(logs.subList(position, logs.size()));
    }

    @Override
    public synchronized List<LogEntry> findByIndexInRange(int startIndex, int endIndex) {

        int fromPos = -1;

        int toPos = -1;

        for (int i = 0; i < logs.size(); i++) {
            if (logs.get(i).getIndex() == startIndex) {
                fromPos = i;
            } else if (logs.get(i).getIndex() == endIndex) {
                toPos = i;
            }

            if (fromPos != -1 && toPos != -1) {
                break;
            }
        }
        if (fromPos != -1 && toPos != -1) {
            return new ArrayList<>(logs.subList(fromPos, toPos + 1));
        }
        return Collections.emptyList();


    }

    @Override
    public synchronized int getLastLogTerm() {
        return lastLog == null ? 0 : lastLog.getTerm();
    }

    @Override
    public synchronized int getLastLogIndex() {
        return lastLog == null ? 0 : lastLog.getIndex();
    }

    @Override
    public void start() {

    }

    @Override
    public void destroy() {

    }
}
