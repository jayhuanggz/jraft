package com.baichen.jraft.log.impl;

import com.baichen.jraft.log.LogStream;
import com.baichen.jraft.log.model.Inflight;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.value.LogStreamState;
import com.baichen.jraft.membership.Peer;
import com.baichen.jraft.model.AppendRequest;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class LogStreamImpl implements LogStream {

    private Peer peer;

    private volatile LogStreamState state;

    private Map<Integer, Inflight> inflightMap;

    private AtomicReference<FluxSink<Inflight>> inflightSinkRef;

    private Flux<Inflight> inflightFlux;

    private Disposable infligtDisposable;


    public LogStreamImpl(Peer peer) {
        this.peer = peer;
        this.state = LogStreamState.NORMAL;
        this.inflightMap = new HashMap<>(1024);
    }

    @Override
    public boolean append(AppendRequest request) {

        if (inflightMap.containsKey(request.getRequestId())) {
            return false;
        }

        if (state == LogStreamState.BLOCKED_BY_ERROR) {
            return false;
        }

        if (state == LogStreamState.FOLLOWER_INCONSISTENT) {
            //if in consistent state, allow only one inflight
            if (!inflightMap.isEmpty()) {
                return false;
            }
        }

        List<LogEntry> logs = request.getLogs();

        int firstIndex = request.getPrevLogIndex() + 1;

        int lastIndex = 0;
        if (logs == null || logs.isEmpty()) {
            //is heartbeat
            lastIndex = firstIndex;
        } else {
            firstIndex = request.getPrevLogIndex() + 1;
            lastIndex = logs.get(logs.size() - 1).getIndex();
        }


        Inflight inflight = new Inflight(request.getRequestId(), request.getTerm(), firstIndex, lastIndex);
        inflightMap.put(inflight.getRequestId(), inflight);
        inflight.setStartTime(System.currentTimeMillis());
        peer.appendEntries(request);
        return true;
    }

    @Override
    public void failOnInconsistency(int requestId, int firstIndex, int lastIndex, int leaderCommit) {

        state = LogStreamState.FOLLOWER_INCONSISTENT;
        Inflight inflight = inflightMap.remove(requestId);

        if (inflight != null) {
            inflight.setSuccess(false);
            inflight.setEndTime(System.currentTimeMillis());
            inflightSinkRef.get().next(inflight);

            // remove newer logs, this happens when a log is rejected
            // while other logs (mostly newer logs as logs are sent in order)
            // are still inflight.
            // No need to wait for newer logs to reply as we are sure
            // at this point they will be rejected as well, so fail fast
            Collection<Integer> newerLogs = new LinkedList<>();
            for (Map.Entry<Integer, Inflight> entry : inflightMap.entrySet()) {
                if (entry.getValue().getLastIndex() > inflight.getLastIndex()) {
                    newerLogs.add(entry.getValue().getRequestId());
                }
            }
            if (!newerLogs.isEmpty()) {
                for (Integer id : newerLogs) {
                    Inflight olderInflight = inflightMap.remove(id);
                    olderInflight.setSuccess(false);
                    olderInflight.setEndTime(System.currentTimeMillis());
                    inflightSinkRef.get().next(olderInflight);

                }
            }

        }

    }

    @Override
    public void onLogAccepted(int requestId, int firstIndex, int lastIndex, int leaderCommit) {

        state = LogStreamState.NORMAL;
        Inflight inflight = inflightMap.remove(requestId);

        if (inflight != null) {
            inflight.setSuccess(true);
            inflight.setEndTime(System.currentTimeMillis());
            inflightSinkRef.get().next(inflight);
        }
    }

    @Override
    public LogStreamState getState() {
        return state;
    }

    @Override
    public Flux<Inflight> getInflights() {
        return inflightFlux;
    }


    @Override
    public void start() {

        this.inflightSinkRef = new AtomicReference<>();
        inflightFlux = Flux.create(sink -> inflightSinkRef.set(sink));
        infligtDisposable = inflightFlux.subscribe();
    }


    @Override
    public void destroy() {


        if (infligtDisposable != null) {
            infligtDisposable.dispose();
        }
    }
}
