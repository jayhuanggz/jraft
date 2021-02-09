package com.baichen.jraft.membership;

import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.Lifecycle;
import com.baichen.jraft.model.VoteRequest;
import com.baichen.jraft.transport.RpcResult;
import com.baichen.jraft.transport.TransportClient;
import com.baichen.jraft.value.NodeInfo;
import com.google.common.util.concurrent.ListenableFuture;

public interface Peer extends Lifecycle {

    NodeInfo getNodeInfo();

    TransportClient getTransport();

    ListenableFuture<RpcResult> appendEntries(AppendRequest request);

    ListenableFuture<RpcResult> requestVote(VoteRequest request);

    void subscribe(PeerListener sub);

    void unsubscribe(PeerListener sub);


}
