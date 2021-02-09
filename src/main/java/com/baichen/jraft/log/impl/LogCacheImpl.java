package com.baichen.jraft.log.impl;

import com.baichen.jraft.log.LogCache;
import com.baichen.jraft.log.model.LogEntry;

import java.util.Collection;

public class LogCacheImpl implements LogCache {

    private static final LogEntry[] NULL = new LogEntry[0];

    private int initialSize;

    private LogEntry[] cache;

    private int size;

    private int lastIndex = -1;


    public LogCacheImpl(int initialSize) {
        this.initialSize = initialSize;
        if (this.initialSize <= 0) {
            this.initialSize = 1024;
        }
        cache = new LogEntry[this.initialSize];
    }

    public void setInitialSize(int initialSize) {
        this.initialSize = initialSize;
    }

    private boolean isFull() {

        return size >= cache.length;
    }


    private void reset() {
        cache = new LogEntry[initialSize];
        size = 0;
        lastIndex = -1;

    }


    private void doubleCache() {

        int length = cache.length;

        int newLength = length << 1;

        LogEntry[] newCache = new LogEntry[newLength];

        System.arraycopy(cache, 0, newCache, 0, size);

        cache = newCache;
    }


    @Override
    public void append(LogEntry log) {

        if (isFull()) {
            doubleCache();
        }
        cache[++lastIndex] = log;
        size++;
    }

    @Override
    public void append(Collection<LogEntry> logs) {
        for (LogEntry log : logs) {
            append(log);
        }
    }

    @Override
    public LogEntry getLastLog() {
        if (size == 0) {
            return null;
        }
        return cache[lastIndex];
    }


    @Override
    public LogEntry getFirstLog() {
        if (size == 0) {
            return null;
        }
        return cache[0];
    }

    @Override
    public LogEntry findByIndex(int index) {
        if (size == 0) {
            return null;
        }
        int position = findPositionByLogIndex(0, size - 1, index);
        return position == -1 ? null : cache[position];
    }

    private int findPositionByLogIndex(int lower, int upper, int index) {

        if (size == 0) {
            return -1;
        }

        if (upper == lower) {
            return cache[lower].getIndex() == index ? lower : -1;
        }

        int mid = lower + ((upper - lower + 1) >> 1);

        LogEntry midLog = cache[mid];
        if (midLog.getIndex() == index) {
            return mid;
        } else if (midLog.getIndex() > index) {
            upper = Math.max(0, mid - 1);
            return findPositionByLogIndex(lower, upper, index);
        } else {
            lower = Math.min(mid + 1, size - 1);
            return findPositionByLogIndex(lower, upper, index);
        }

    }

    private int findClosetPositionAfterLogIndex(int lower, int upper, int index) {

        if (size == 0) {
            return -1;
        }

        if (upper == lower) {
            return cache[lower].getIndex() >= index ? lower : -1;
        }

        int mid = lower + ((upper - lower + 1) >> 1);

        LogEntry midLog = cache[mid];
        if (midLog.getIndex() == index) {
            return mid;
        } else if (midLog.getIndex() > index) {
            upper = Math.max(0, mid - 1);
            return findClosetPositionAfterLogIndex(lower, upper, index);
        } else {
            lower = Math.min(mid + 1, size - 1);
            return findClosetPositionAfterLogIndex(lower, upper, index);
        }

    }

    private int findClosetPositionBeforeLogIndex(int lower, int upper, int index) {

        if (size == 0) {
            return -1;
        }

        if (upper == lower) {
            return cache[lower].getIndex() <= index ? lower : -1;
        }

        int mid = lower + ((upper - lower + 1) >> 1);

        LogEntry midLog = cache[mid];
        if (midLog.getIndex() == index) {
            return mid;
        } else if (midLog.getIndex() > index) {
            upper = Math.max(0, mid - 1);
            return findClosetPositionBeforeLogIndex(lower, upper, index);
        } else {
            lower = Math.min(mid + 1, size - 1);
            return findClosetPositionBeforeLogIndex(lower, upper, index);
        }

    }


    @Override
    public int deleteLogsStartingFrom(int startIndex) {

        if (size == 0) {
            return 0;
        }

        int position = findClosetPositionAfterLogIndex(0, size - 1, startIndex);

        if (position == -1) {
            return 0;
        }
        int length = lastIndex - position + 1;

        for (int i = position; i <= lastIndex; i++) {
            cache[position] = null;
        }
        size -= length;
        lastIndex = size - 1;
        return length;

    }

    @Override
    public int deleteLogsBefore(int endIndex) {

        if (size == 0) {
            return 0;
        }


        int position = findClosetPositionBeforeLogIndex(0, size - 1, endIndex);
        if (position == -1) {
            return 0;
        }

        // delete all
        if (position >= size - 1) {
            int deleted = size;
            reset();
            return deleted;
        }
        int newSize = size - position - 1;
        int newLength = Math.max(initialSize, newSize);

        LogEntry[] newCache = new LogEntry[newLength];

        System.arraycopy(cache, position + 1, newCache, 0, newSize);
        this.cache = newCache;
        this.size = newSize;
        lastIndex = newSize - 1;
        return position + 1;
    }

    @Override
    public LogEntry[] findByIndexStartingFrom(int startIndex) {

        if (size == 0) {
            return NULL;
        }

        int position = findClosetPositionAfterLogIndex(0, size - 1, startIndex);

        if (position == -1) {
            return NULL;
        }

        int size = lastIndex - position + 1;

        LogEntry[] result = new LogEntry[size];
        System.arraycopy(cache, position, result, 0, lastIndex - position + 1);
        return result;
    }

    @Override
    public int size() {
        return size;
    }


    @Override
    public void start() {

    }

    @Override
    public void destroy() {
        cache = null;
    }
}
