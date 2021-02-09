package com.baichen.jraft.log.model;

import java.io.Serializable;

public class Inflight implements Serializable {

    private int requestId;

    private int term;

    private int firstIndex;

    private int lastIndex;

    private boolean success;

    private long startTime;

    private long endTime;

    public Inflight(int requestId, int term, int firstIndex, int lastIndex) {
        this.requestId = requestId;
        this.term = term;
        this.firstIndex = firstIndex;
        this.lastIndex = lastIndex;
    }

    public int getRequestId() {
        return requestId;
    }

    public int getTerm() {
        return term;
    }

    public int getFirstIndex() {
        return firstIndex;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public boolean isSuccess() {
        return success;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }
}
