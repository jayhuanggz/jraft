package com.baichen.jraft;

public interface Candidate extends ServerStateInternal {

    void handleVoteReplyFromPeer(String nodeId);
}
