package com.baichen.jraft.model;

public class VoteRequest extends RpcMessage {


    private String candidateId;

    private int lastLogIndex;

    private int lastLogTerm;

    public VoteRequest(int requestId, int term, String candidateId, int lastLogIndex, int lastLogTerm) {
        this.requestId = requestId;
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }


    public String getCandidateId() {
        return candidateId;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }
}
