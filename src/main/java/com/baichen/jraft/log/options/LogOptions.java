package com.baichen.jraft.log.options;

public class LogOptions {

    private LogCacheOptions cache = new LogCacheOptions();

    private LogStorageOptions storage = new LogStorageOptions();

    private LogBatchOptions batch = new LogBatchOptions();

    private int disruptorBufferSize = 1024;

    public int getDisruptorBufferSize() {
        return disruptorBufferSize;
    }

    public void setDisruptorBufferSize(int disruptorBufferSize) {
        this.disruptorBufferSize = disruptorBufferSize;
    }


    public LogCacheOptions getCache() {
        return cache;
    }

    public void setCache(LogCacheOptions cache) {
        this.cache = cache;
    }

    public LogStorageOptions getStorage() {
        return storage;
    }

    public void setStorage(LogStorageOptions storage) {
        this.storage = storage;
    }

    public LogBatchOptions getBatch() {
        return batch;
    }

    public void setBatch(LogBatchOptions batch) {
        this.batch = batch;
    }
}
