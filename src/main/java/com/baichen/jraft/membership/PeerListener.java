package com.baichen.jraft.membership;

import com.baichen.jraft.transport.RpcResult;

public interface PeerListener {

    void onVoteReply(RpcResult result);

    void onAppendReply(RpcResult result);
}
