package com.baichen.jraft;

import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.model.VoteRequest;
import com.baichen.jraft.transport.RpcResult;

public interface Follower extends ServerStateInternal {

    RpcResult handleAppendReceived(AppendRequest request);

    RpcResult handleRequestVoteReceived(VoteRequest request);
}
