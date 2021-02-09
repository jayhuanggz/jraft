package com.baichen.jraft.meta;

public class PersistentStateImpl implements PersistentState {

    private volatile int term;

    private volatile String votedFor;

    private int votedForTerm;


    public PersistentStateImpl(int term, String votedFor, int votedForTerm) {
        this.term = term;
        this.votedFor = votedFor;
        this.votedForTerm = votedForTerm;
    }

    public PersistentStateImpl() {
    }

    public void setTerm(int term) {
        this.term = term;
    }


    public void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }


    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public String getVotedFor() {
        return votedFor;
    }

    @Override
    public int getVotedForTerm() {
        return votedForTerm;
    }

    public void setVotedForTerm(int votedForTerm) {
        this.votedForTerm = votedForTerm;
    }
}
