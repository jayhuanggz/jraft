package com.baichen.jraft.transport.grpc;

import com.baichen.jraft.Lifecycle;
import com.baichen.jraft.util.RepeatableTimer;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.ClientResponseObserver;

/**
 * Auto recoverable client stream
 *
 * @param <T> request type
 */
public interface Stream<T> extends Lifecycle {

    PendingRequest<T> next(int requestId, T request);

    int getPendingRequestCount();

    ClientResponseObserver<T, RpcResult> newObserver();


    final class PendingRequest<T> {

        private final T request;

        private final int requestId;

        private final SettableFuture<com.baichen.jraft.transport.RpcResult> future;

        private RepeatableTimer.TimerTask timeout;

        private long startTime;

        public PendingRequest(T request, int requestId, SettableFuture<com.baichen.jraft.transport.RpcResult> future) {
            this.request = request;
            this.requestId = requestId;
            this.future = future;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public void setTimeout(RepeatableTimer.TimerTask timeout) {
            this.timeout = timeout;
        }

        public RepeatableTimer.TimerTask getTimeout() {
            return timeout;
        }

        public T getRequest() {
            return request;
        }

        public int getRequestId() {
            return requestId;
        }

        public SettableFuture<com.baichen.jraft.transport.RpcResult> getFuture() {
            return future;
        }
    }
}
