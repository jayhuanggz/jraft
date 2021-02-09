package com.baichen.jraft.transport.grpc;

import com.baichen.jraft.transport.exception.TransportClientRequestTimeoutException;
import com.baichen.jraft.transport.exception.UnexpectedEndOfExecutionException;
import com.baichen.jraft.util.RepeatableHashedWheelTimer;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class StreamImpl<T> implements Stream<T> {

    private static final Logger LOGGER = LogManager.getLogger(StreamImpl.class);

    private static final int STATE_ACTIVE = 0;

    private static final int STATE_RECOVER = 1;

    private static final int STATE_CLOSED = 2;

    private static final int STATE_DESTROYED = 3;

    private Lock lock = new ReentrantLock();

    private volatile int state = STATE_ACTIVE;

    private Supplier<StreamObserver<T>> stubSupplier;

    private final Queue<PendingRequest<T>> requestQueue = new LinkedList<>();

    private volatile StreamObserver<T> stub;

    private long defaultRequestTimeout = 5000l;

    private RepeatableHashedWheelTimer requestTimer;

    private int capacity = 1024 * 1024;

    public StreamImpl(Supplier<StreamObserver<T>> stubSupplier) {
        this.stubSupplier = stubSupplier;
    }

    public void setDefaultRequestTimeout(long defaultRequestTimeout) {
        this.defaultRequestTimeout = defaultRequestTimeout;
    }


    @Override
    public PendingRequest<T> next(int requestId, T request) {

        Preconditions.checkArgument(state != STATE_DESTROYED, "Stream has destroyed");

        int spinCount = 0;
        while (capacity <= getPendingRequestCount()) {
            if (++spinCount >= 100) {
                Thread.yield();
            }
        }

        SettableFuture<com.baichen.jraft.transport.RpcResult> future = SettableFuture.create();
        PendingRequest<T> pendingRequest = new PendingRequest<>(request, requestId, future);
        addRequest(pendingRequest);

        return pendingRequest;
    }

    @Override
    public int getPendingRequestCount() {
        return requestQueue.size();
    }

    @Override
    public ClientResponseObserver<T, RpcResult> newObserver() {
        return new ResponseObserver<>(this);
    }

    @Override
    public void start() {
        requestTimer = new RepeatableHashedWheelTimer(100, 10, TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() {
        lock.lock();

        try {
            if (state == STATE_DESTROYED) {
                return;
            }
            state = STATE_DESTROYED;
            if (requestTimer != null) {
                requestTimer.shutdown();
                requestTimer = null;
            }
            failPendingRequests(null);
            requestQueue.clear();
            if (stub != null) {
                stub.onCompleted();
                stub = null;
            }
        } finally {
            lock.unlock();
        }
    }

    private void failPendingRequests(Throwable cause) {
        PendingRequest<T> request;
        while ((request = requestQueue.poll()) != null) {
            if (request.getFuture().isDone() || request.getFuture().isCancelled()) {
                continue;
            }
            if (cause == null) {
                request.getFuture().setException(new UnexpectedEndOfExecutionException(String.format("Grpc stream was closed unexpectedly, failed request id: %s", request.getRequestId(), cause)));
            } else {
                request.getFuture().setException(cause);
            }
        }


    }


    private void addRequest(final PendingRequest<T> request) {
        lock.lock();
        try {
            if (state == STATE_DESTROYED) {
                return;
            }

            // if the stream has closed due to previous failure, try to recover first
            if (state == STATE_CLOSED) {
                state = STATE_RECOVER;
                stub = stubSupplier.get();
                if (state == STATE_CLOSED) {
                    request.getFuture().setException(new UnexpectedEndOfExecutionException("Grpc stream was completed while there are pending requests"));
                    return;
                }
                state = STATE_ACTIVE;
            }

            if (stub == null) {
                stub = stubSupplier.get();
            }
            requestQueue.offer(request);
            request.setTimeout(requestTimer.schedule(() -> {
                handleRequestTimeout(request);
            }, () -> defaultRequestTimeout));
            request.setStartTime(System.currentTimeMillis());
            stub.onNext(request.getRequest());

        } finally {
            lock.unlock();
        }
    }

    private void handleRequestTimeout(PendingRequest<T> request) {

        if (state == STATE_DESTROYED) {
            return;
        }
        if (request.getFuture().isCancelled() || request.getFuture().isDone()) {
            return;
        }

        long duration = System.currentTimeMillis() - request.getStartTime();
        request.getFuture().setException(
                new TransportClientRequestTimeoutException(String.format("Request %s  has timed out, duration: %s", request.getRequestId(), duration)));


    }

    private void handleReply(com.baichen.jraft.transport.RpcResult result) {

        lock.lock();

        try {
            if (state == STATE_DESTROYED) {
                return;
            }
            PendingRequest<T> request;

            if ((request = requestQueue.peek()) != null) {

                if (request.getRequestId() == result.getRequestId()) {
                    if (request.getTimeout() != null) {
                        request.getTimeout().cancel();
                    }
                    requestQueue.poll();

                    if (!request.getFuture().isDone() && !request.getFuture().isCancelled()) {
                        request.getFuture().set(result);
                    }
                } else {
                    LOGGER.warn("Received reply with requestId {}, does not match the top pending request {} in the queue, something might have " +
                            "gone wrong, will ignore this reply", result.getRequestId(), request.getRequestId());
                }
            }


        } finally {
            lock.unlock();
        }
    }

    private void recover(Throwable cause) {

        lock.lock();

        try {

            if (state == STATE_DESTROYED) {
                return;
            }

            if (state == STATE_RECOVER) {
                // stream already closed, this is a typical case when the previous
                // stream closed due to grpc load balancer failed to connect to any server,
                // then it is trying to re-create a stream immediately
                state = STATE_CLOSED;
                return;
            }

            if (state == STATE_CLOSED) {
                return;
            }

            if (state == STATE_ACTIVE) {
                state = STATE_RECOVER;
                failPendingRequests(cause);
                stub = stubSupplier.get();
                if (state != STATE_CLOSED) {
                    state = STATE_ACTIVE;
                }
            }

        } finally {
            lock.unlock();
        }


    }


    static class ResponseObserver<T> implements ClientResponseObserver<T, RpcResult> {

        private StreamImpl<T> stream;

        public ResponseObserver(StreamImpl<T> stream) {
            this.stream = stream;
        }

        @Override
        public void onNext(com.baichen.jraft.transport.grpc.RpcResult result) {
            stream.handleReply(GrpcUtils.toJavaResult(result));
        }

        @Override
        public void onError(Throwable t) {
            LOGGER.warn("onError called on stream: {}", t.getMessage());
            stream.recover(t);
            stream = null;

        }

        @Override
        public void onCompleted() {
            stream.recover(null);
            stream = null;

        }

        @Override
        public void beforeStart(ClientCallStreamObserver<T> requestStream) {
        }
    }

}
