package com.baichen.jraft.model;

import java.io.Serializable;

public final class VolatileState implements Serializable {

    private volatile int commitIndex;

    private volatile int lastApplied;

    public VolatileState(int commitIndex, int lastApplied) {
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public int getLastApplied() {
        return lastApplied;
    }

    public void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }

    public void setLastApplied(int lastApplied) {
        this.lastApplied = lastApplied;
    }


}
