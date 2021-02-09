package com.baichen.jraft.transport;


import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.model.VoteRequest;

public interface TransportServerService {

    RpcResult handleAppendRequest(AppendRequest request);

    RpcResult handleVoteRequest(VoteRequest request);

}
