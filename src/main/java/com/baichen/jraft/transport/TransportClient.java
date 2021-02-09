package com.baichen.jraft.transport;

import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.Lifecycle;
import com.baichen.jraft.model.VoteRequest;
import com.baichen.jraft.value.NodeInfo;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A rpc client that connects to a peer.
 */
public interface TransportClient extends Lifecycle {

    NodeInfo getPeer();

    ListenableFuture<RpcResult> sendVoteRequest(VoteRequest request);

    ListenableFuture<RpcResult> sendAppendRequest(AppendRequest request);

    void subscribe(TransportClientListener sub);

    void unsubscribe(TransportClientListener sub);

    TransportClientState getState();


}
