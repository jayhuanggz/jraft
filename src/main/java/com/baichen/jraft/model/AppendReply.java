package com.baichen.jraft.model;

public class AppendReply extends Reply {

    private int commitIndex;

    private boolean inconsistent;

    public AppendReply(String nodeId, int term, boolean success, int commitIndex) {
        super(nodeId, term, success);
        this.commitIndex = commitIndex;
    }

    public AppendReply(String nodeId, int term, boolean success) {
        super(nodeId, term, success);
    }

    public boolean isInconsistent() {
        return inconsistent;
    }

    public void setInconsistent(boolean inconsistent) {
        this.inconsistent = inconsistent;
    }

    public AppendReply() {
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }
}
