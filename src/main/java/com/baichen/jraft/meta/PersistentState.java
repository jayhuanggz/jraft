package com.baichen.jraft.meta;

public interface PersistentState {

    int getTerm();

    String getVotedFor();

    int getVotedForTerm();


}
