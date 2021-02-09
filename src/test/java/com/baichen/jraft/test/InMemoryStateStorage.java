package com.baichen.jraft.test;

import com.baichen.jraft.meta.PersistentState;
import com.baichen.jraft.meta.PersistentStateImpl;
import com.baichen.jraft.meta.StateStorage;

public class InMemoryStateStorage implements StateStorage {

    private PersistentStateImpl state = new PersistentStateImpl();

    @Override
    public PersistentState getState() {
        return state;
    }

    @Override
    public void updateTerm(int term) {
        state.setTerm(term);
    }

    @Override
    public int incrementTerm() {
        int term = state.getTerm();
        term++;
        state.setTerm(term);
        return term;
    }

    @Override
    public void updateVoteFor(String candidateId, int term) {
        state.setVotedFor(candidateId);
        state.setVotedForTerm(term);
    }

    @Override
    public void start() {

    }

    @Override
    public void destroy() {

    }
}
