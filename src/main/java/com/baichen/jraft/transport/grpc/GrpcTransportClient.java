package com.baichen.jraft.transport.grpc;

import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.model.VoteRequest;
import com.baichen.jraft.transport.RpcResult;
import com.baichen.jraft.transport.TransportClient;
import com.baichen.jraft.transport.TransportClientListener;
import com.baichen.jraft.transport.TransportClientState;
import com.baichen.jraft.value.NodeInfo;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class GrpcTransportClient implements TransportClient {

    private static final Logger LOGGER = LogManager.getLogger(GrpcTransportClient.class);

    private NodeInfo nodeInfo;

    private GrpcTransportServiceGrpc.GrpcTransportServiceStub stub;

    private ManagedChannel channel;

    private volatile TransportClientState state;

    private Stream<com.baichen.jraft.transport.grpc.VoteRequest> voteStream;

    private Stream<com.baichen.jraft.transport.grpc.AppendRequest> appendStream;

    private List<TransportClientListener> subs = new CopyOnWriteArrayList<>();

    public GrpcTransportClient(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
        this.state = TransportClientState.NOT_STARTED;
    }

    @Override
    public NodeInfo getPeer() {
        return nodeInfo;
    }

    @Override
    public void subscribe(TransportClientListener sub) {
        this.subs.add(sub);
    }

    @Override
    public void unsubscribe(TransportClientListener sub) {
        subs.remove(sub);
    }

    @Override
    public TransportClientState getState() {
        return this.state;
    }

    @Override
    public void start() {

        synchronized (this) {

            if (state != TransportClientState.NOT_STARTED) {
                return;
            }

            state = TransportClientState.STARTING;

            notifyStateChange(TransportClientState.NOT_STARTED, TransportClientState.STARTING);

            channel = NettyChannelBuilder
                    .forAddress(nodeInfo.getHost(), nodeInfo.getPort()).enableRetry()
                    .maxRetryAttempts(Integer.MAX_VALUE)
                    .usePlaintext().directExecutor()
                    .build();
            stub = GrpcTransportServiceGrpc.newStub(channel);
            state = TransportClientState.STARTED;

            voteStream = new StreamImpl<>(() ->
                    stub.sendVote(voteStream.newObserver()));
            voteStream.start();

            appendStream = new StreamImpl<>(() ->
                    stub.sendAppend(appendStream.newObserver()));
            appendStream.start();

            notifyStateChange(TransportClientState.STARTING, TransportClientState.STARTED);
        }

    }

    @Override
    public ListenableFuture<RpcResult> sendVoteRequest(VoteRequest request) {

        Preconditions.checkArgument(state != TransportClientState.DESTROYED, "GrpcTransportClient has destroyed!");

        Stream<com.baichen.jraft.transport.grpc.VoteRequest> streamCopy = this.voteStream;
        Preconditions.checkArgument(streamCopy != null);
        Stream.PendingRequest<com.baichen.jraft.transport.grpc.VoteRequest> pending = streamCopy.next(request.getRequestId(), GrpcUtils.toRpcVoteRequest(request));
        return pending.getFuture();

    }

    @Override
    public ListenableFuture<RpcResult> sendAppendRequest(AppendRequest request) {
        Preconditions.checkArgument(state != TransportClientState.DESTROYED, "GrpcTransportClient has destroyed!");

        Stream<com.baichen.jraft.transport.grpc.AppendRequest> streamCopy = this.appendStream;
        Preconditions.checkArgument(streamCopy != null);
        Stream.PendingRequest<com.baichen.jraft.transport.grpc.AppendRequest> pending = streamCopy.next(request.getRequestId(), GrpcUtils.toRpcAppendRequest(request));
        return pending.getFuture();
    }

    private void notifyStateChange(TransportClientState oldState, TransportClientState newState) {
        for (TransportClientListener sub : subs) {
            sub.onStateChanged(this, oldState, newState);
        }
    }


    @Override
    public void destroy() {

        synchronized (this) {

            if (state == TransportClientState.DESTROYED) {
                return;
            }

            TransportClientState oldState = state;

            state = TransportClientState.DESTROYED;

            notifyStateChange(oldState, TransportClientState.DESTROYED);

            subs.clear();
            if (appendStream != null) {
                try {
                    appendStream.destroy();
                } catch (Throwable e) {
                    LOGGER.warn(e.getMessage(), e);
                } finally {
                    appendStream = null;
                }
            }
            if (voteStream != null) {
                try {
                    voteStream.destroy();
                } catch (Throwable e) {
                    LOGGER.warn(e.getMessage(), e);
                } finally {
                    voteStream = null;
                }
            }

            if (channel != null) {
                channel.shutdownNow();
                try {
                    while (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
                        LOGGER.info("Waiting channel to shutdown for 1 sec......");
                    }
                } catch (InterruptedException e) {

                }
            }
        }

    }


}
