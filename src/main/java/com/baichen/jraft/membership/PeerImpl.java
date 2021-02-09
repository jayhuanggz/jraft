package com.baichen.jraft.membership;

import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.model.VoteRequest;
import com.baichen.jraft.transport.RpcResult;
import com.baichen.jraft.transport.TransportClient;
import com.baichen.jraft.transport.TransportFactory;
import com.baichen.jraft.transport.TransportOptions;
import com.baichen.jraft.value.NodeInfo;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class PeerImpl implements Peer {

    private TransportFactory transportFactory;

    private NodeInfo nodeInfo;

    private AtomicInteger state;

    private TransportOptions transportOptions;

    private TransportClient transportClient;

    private List<PeerListener> subs = new CopyOnWriteArrayList<>();

    private ExecutorService executor;

    public PeerImpl(NodeInfo nodeInfo, TransportFactory transportFactory) {
        this.nodeInfo = nodeInfo;
        this.transportFactory = transportFactory;
        executor = Executors.newSingleThreadExecutor();
    }

    @Override
    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    @Override
    public TransportClient getTransport() {
        return transportClient;
    }

    @Override
    public ListenableFuture<RpcResult> appendEntries(AppendRequest request) {

        ListenableFuture<RpcResult> future = transportClient.sendAppendRequest(request);
        Futures.addCallback(future, new FutureCallback<RpcResult>() {
            @Override
            public void onSuccess(@NullableDecl RpcResult result) {
                for (PeerListener sub : subs) {
                    sub.onAppendReply(result);
                }
            }

            @Override
            public void onFailure(Throwable t) {

            }
        }, executor);
        return future;
    }

    @Override
    public ListenableFuture<RpcResult> requestVote(VoteRequest request) {
        ListenableFuture<RpcResult> future = transportClient.sendVoteRequest(request);
        Futures.addCallback(future, new FutureCallback<RpcResult>() {
            @Override
            public void onSuccess(@NullableDecl RpcResult result) {
                for (PeerListener sub : subs) {
                    sub.onVoteReply(result);
                }
            }

            @Override
            public void onFailure(Throwable t) {

            }
        }, executor);

        return future;
    }

    @Override
    public void subscribe(PeerListener sub) {
        subs.add(sub);
    }

    @Override
    public void unsubscribe(PeerListener sub) {
        subs.remove(sub);
    }

    @Override
    public void start() {

        transportClient = transportFactory.createClient(transportOptions, nodeInfo);
        transportClient.start();

    }

    @Override
    public void destroy() {
        subs.clear();

        if (transportClient != null) {
            transportClient.destroy();
        }
        executor.shutdownNow();
        executor = null;

        transportFactory = null;
        transportOptions = null;

    }
}
