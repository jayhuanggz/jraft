package com.baichen.jraft.log;

import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.Lifecycle;
import com.baichen.jraft.log.model.Inflight;
import com.baichen.jraft.log.value.LogStreamState;
import reactor.core.publisher.Flux;

public interface LogStream extends Lifecycle {

    /**
     * Append new log(s) to the stream. It is guaranteed that logs are sent
     * in strict order.
     * <p>
     * In the case when follower is replicating
     * while new logs come in at the same time, logs will be rejected by follower and
     * the stream's state will become {@code com.baichen.jraft.log.value.LogStreamState.FOLLOWER_INCONSISTENT},
     * new logs(including heartbeats) will be rejected until replication completes to prevent
     * back pressure by retry logs
     * </p>
     *
     * @param request
     * @return false if rejected
     */
    boolean append(AppendRequest request);

    /**
     * Handle the case when a log is rejected by peer because of inconsistency.
     * This typically happens when the follower is replicating,
     * the stream's state becomes {@code com.baichen.jraft.log.value.LogStreamState.FOLLOWER_INCONSISTENT},
     * new logs will be rejected
     *
     * @param requestId
     * @param firstIndex   index of first log shipped
     * @param lastIndex    index of last log shipped
     * @param leaderCommit current commit index
     */
    void failOnInconsistency(int requestId, int firstIndex, int lastIndex, int leaderCommit);

    /**
     * When a log is accepted by follower,
     * the stream's state becomes {@code com.baichen.jraft.log.value.LogStreamState.NORMAL},
     * and back to normal operation
     *
     * @param requestId
     * @param firstIndex   index of first log shipped
     * @param lastIndex    index of last log shipped
     * @param leaderCommit current commit index
     */
    void onLogAccepted(int requestId, int firstIndex, int lastIndex, int leaderCommit);

    LogStreamState getState();

    Flux<Inflight> getInflights();


}
