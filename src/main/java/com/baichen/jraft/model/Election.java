package com.baichen.jraft.model;

import com.baichen.jraft.Lifecycle;

public interface Election extends Lifecycle {

    int getTerm();

    boolean wins();

    void receiveVoteReply(String nodeId);

    int getGrantedCount();

}
