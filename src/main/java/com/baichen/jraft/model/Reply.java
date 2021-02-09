package com.baichen.jraft.model;

import java.io.Serializable;

public class Reply implements Serializable {

    private String nodeId;

    private int term;

    private boolean success;

    public Reply(String nodeId, int term, boolean success) {
        this.nodeId = nodeId;
        this.term = term;
        this.success = success;
    }

    public Reply() {
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }
}
