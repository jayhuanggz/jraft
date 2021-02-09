package com.baichen.jraft.log.options;

public class LogCacheOptions {

    private int initialSize = 1024;

    private int maxSizeHint = 1024 * 1024;

    public int getInitialSize() {
        return initialSize;
    }

    public void setInitialSize(int initialSize) {
        this.initialSize = initialSize;
    }

    public int getMaxSizeHint() {
        return maxSizeHint;
    }

    public void setMaxSizeHint(int maxSizeHint) {
        this.maxSizeHint = maxSizeHint;
    }
}
