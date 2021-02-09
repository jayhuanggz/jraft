package com.baichen.jraft;

import java.io.Serializable;

public class RaftResponse implements Serializable {

    private int index;

    private int term;

    private int leaderCommit;

    private boolean success;

    private String msg;

    public RaftResponse(int index, int term, int leaderCommit) {
        this.index = index;
        this.term = term;
        this.leaderCommit = leaderCommit;
        this.success = true;
    }

    public RaftResponse(int index, int term, int leaderCommit, boolean success, String msg) {
        this.index = index;
        this.term = term;
        this.leaderCommit = leaderCommit;
        this.success = success;
        this.msg = msg;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public void setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }
}
