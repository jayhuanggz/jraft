package com.baichen.jraft.log.options;

public class LogBatchOptions {

    private int batchSize = 100;

    private int flushTimeout = 1000;

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getFlushTimeout() {
        return flushTimeout;
    }

    public void setFlushTimeout(int flushTimeout) {
        this.flushTimeout = flushTimeout;
    }
}
