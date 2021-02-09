package com.baichen.jraft.log.impl;

import com.baichen.jraft.log.model.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class DefaultLogBatcher extends AbstractLogBatcher {

    private int batchSize;

    private int timeout;

    private final List<LogEntry> batch;

    private Timer timer;

    private volatile long firstEntryTime;

    public DefaultLogBatcher(int batchSize, int timeout) {

        assert batchSize > 0;
        assert timeout > 100;

        this.batchSize = batchSize;
        this.timeout = timeout;
        this.batch = new ArrayList<>(batchSize);
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    @Override
    protected List<LogEntry> getBatch() {
        return batch;
    }

    @Override
    protected boolean shouldFlush() {
        boolean full = batch.size() >= batchSize;
        if (full) {
            return true;
        }

        return firstEntryTime + timeout <= System.currentTimeMillis();
    }

    @Override
    protected void doAdd(LogEntry log) {

        super.doAdd(log);
        if (firstEntryTime == 0) {
            firstEntryTime = System.currentTimeMillis();
        }

    }

    @Override
    protected void onFlush() {
        super.onFlush();
        firstEntryTime = 0;
    }

    @Override
    protected void cleanUp() {

        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }

    @Override
    public void start() {
        super.start();
        timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
                                      @Override
                                      public void run() {
                                          flushIfReady();
                                      }
                                  },
                timeout / 2, timeout / 2);


    }
}
