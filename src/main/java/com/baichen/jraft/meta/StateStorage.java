package com.baichen.jraft.meta;

import com.baichen.jraft.Lifecycle;

public interface StateStorage extends Lifecycle {

    PersistentState getState();

    void updateTerm(int term);

    int incrementTerm();

    void updateVoteFor(String candidateId, int term);


}
