package com.baichen.jraft.transport.grpc;

import com.baichen.jraft.transport.*;
import com.baichen.jraft.value.NodeInfo;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class GrpcTransportServer implements TransportServer {

    private static final Logger LOGGER = LogManager.getLogger(GrpcTransportServer.class);

    private NodeInfo nodeInfo;

    private TransportServerService service;

    private Server server;

    private ExceptionHandler exceptionHandler = new DefaultExceptionHandler();

    private AtomicReference<TransportServerState> state;

    public GrpcTransportServer(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
        this.state = new AtomicReference<>(TransportServerState.NOT_STARTED);
    }

    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public void registerService(TransportServerService service) {
        assert this.service == null;
        this.service = service;
    }

    @Override
    public TransportServerState getState() {
        return state.get();
    }

    @Override
    public ListenableFuture<Void> startWithFuture() {
        SettableFuture<Void> startFuture = SettableFuture.create();
        doStart(startFuture);
        return startFuture;
    }

    private void doStart(SettableFuture<Void> future) {
        if (state.compareAndSet(TransportServerState.NOT_STARTED, TransportServerState.STARTING)) {
            server = ServerBuilder.forPort(nodeInfo.getPort()).addService(new ServerServiceImpl(this)).build();
            Thread startThread = new Thread(() -> startServer(future));
            startThread.setDaemon(true);
            startThread.start();
        }
    }


    @Override
    public void start() {
        doStart(null);
    }

    private void startServer(SettableFuture<Void> future) {
        try {
            if (state.get() != TransportServerState.DESTROYED) {
                server.start();
                LOGGER.info("Grpc server started successfully at port {}", nodeInfo.getPort());
                state.compareAndSet(TransportServerState.STARTING, TransportServerState.STARTED);
                if (future != null) {
                    future.set(null);
                }
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        try {
                            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            e.printStackTrace(System.err);
                        }
                    }
                });

            }

        } catch (IOException e) {
            LOGGER.warn("Start Grpc server at port " + nodeInfo.getPort() + " failed: {}, retry in 1 second", e.getMessage());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {

            }
            startServer(future);
        }
    }

    private void awaitServerShutdown() {

        while (!server.isTerminated()) {
            try {
                server.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {

            }

        }

    }

    @Override
    public void destroy() {

        TransportServerState curState;
        while ((curState = state.get()) != TransportServerState.DESTROYED) {
            if (state.compareAndSet(curState, TransportServerState.DESTROYED)) {
                if (server != null) {
                    server.shutdownNow();
                    awaitServerShutdown();
                }

            }
        }


    }


    private static final class ServerServiceImpl extends GrpcTransportServiceGrpc.GrpcTransportServiceImplBase {

        private GrpcTransportServer server;

        public ServerServiceImpl(GrpcTransportServer server) {
            this.server = server;
        }

        @Override
        public StreamObserver<AppendRequest> sendAppend(StreamObserver<RpcResult> responseObserver) {
            return new StreamObserver<AppendRequest>() {
                @Override
                public void onNext(AppendRequest value) {
                    com.baichen.jraft.transport.RpcResult result;
                    try {
                        result = server.service.handleAppendRequest(GrpcUtils.toJavaAppendRequest(value));
                    } catch (Throwable e) {
                        result = server.exceptionHandler.handle(value.getRequestId(), server.nodeInfo, e);
                    }
                    responseObserver.onNext(GrpcUtils.toRpcResult(result));
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            };
        }

        @Override
        public StreamObserver<VoteRequest> sendVote(StreamObserver<RpcResult> responseObserver) {
            return new StreamObserver<VoteRequest>() {
                @Override
                public void onNext(VoteRequest value) {
                    com.baichen.jraft.transport.RpcResult result;
                    try {
                        result = server.service.handleVoteRequest(GrpcUtils.toJavaVoteRequest(value));
                    } catch (Throwable e) {
                        result = server.exceptionHandler.handle(value.getRequestId(), server.nodeInfo, e);
                    }
                    responseObserver.onNext(GrpcUtils.toRpcResult(result));
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                }

                @Override
                public void onCompleted() {

                }
            };
        }
    }

}
