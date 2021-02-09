package com.baichen.jraft.log.impl;

import com.baichen.jraft.log.LogBatcher;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.util.NamedThreadFactory;
import com.baichen.jraft.util.Subscription;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.ThreadHints;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public abstract class AbstractLogBatcher implements LogBatcher {

    private static final Logger LOGGER = LogManager.getLogger(AbstractLogBatcher.class);

    private static final EventTranslatorOneArg<DispatchEvent, Collection<LogEntry>> TRANSLATOR = new EventTranslatorOneArg<DispatchEvent, Collection<LogEntry>>() {
        @Override
        public void translateTo(DispatchEvent event, long sequence, Collection<LogEntry> logs) {
            event.setLogs(logs);
        }
    };

    private List<Consumer<Collection<LogEntry>>> subs = new CopyOnWriteArrayList<>();

    private ReentrantLock lock = new ReentrantLock();

    private RingBuffer<DispatchEvent> ringBuffer;

    private Disruptor<DispatchEvent> disruptor;

    private int ringBufferSize = 1024;

    private static class DispatchEvent {

        private Collection<LogEntry> logs;

        public Collection<LogEntry> getLogs() {
            return logs;
        }

        public void setLogs(Collection<LogEntry> logs) {
            this.logs = logs;
        }
    }

    @Override
    public void add(LogEntry log) {
        lock.lock();
        try {
            doAdd(log);
        } finally {
            lock.unlock();
        }
    }


    @Override
    public void add(Collection<LogEntry> logs) {
        lock.lock();
        try {
            for (LogEntry log : logs) {
                doAdd(log);
            }
        } finally {
            lock.unlock();
        }
    }

    protected void doAdd(LogEntry log) {
        getBatch().add(log);
        flushIfReady();
    }

    protected void flushIfReady() {

        if (shouldFlush()) {
            flush();
        }
    }


    @Override
    public void flush() {

        lock.lock();

        try {
            onFlush();

            List<LogEntry> batch = getBatch();

            if (!batch.isEmpty()) {
                List<LogEntry> copy = Collections.unmodifiableList(new ArrayList<>(batch));
                batch.clear();

                while (!ringBuffer.tryPublishEvent(TRANSLATOR, copy)) {
                    ThreadHints.onSpinWait();
                }
            }

            long end = System.currentTimeMillis();


        } finally {
            lock.unlock();
        }
    }


    @Override
    public Subscription subscribe(Consumer<Collection<LogEntry>> sub) {
        lock.lock();
        try {
            subs.add(sub);
            return () -> subs.remove(sub);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void start() {


        disruptor = new Disruptor<>(() -> new DispatchEvent(),
                ringBufferSize,
                new NamedThreadFactory("JRaft-AbstractLogBatcher", true),
                ProducerType.SINGLE, new YieldingWaitStrategy());

        disruptor.handleEventsWith(new EventHandler<DispatchEvent>() {
            @Override
            public void onEvent(DispatchEvent event, long sequence, boolean endOfBatch) throws Exception {
                try {
                    Collection<LogEntry> logs = event.getLogs();
                    LOGGER.debug("Dispatching {} logs in batch", logs.size());
                    for (Consumer<Collection<LogEntry>> sub : subs) {
                        sub.accept(logs);
                    }
                } catch (Throwable e) {
                    LOGGER.warn("Error dispatching logs", e);
                }
            }
        });
        ringBuffer = disruptor.start();

    }

    private void waitForDispatch() {


        long size = ringBufferSize - ringBuffer.remainingCapacity();

        if (size > 0) {
            LOGGER.info("Wait for dispatch {} logs", size);
        }
        while (ringBuffer.remainingCapacity() < ringBufferSize) {
            ThreadHints.onSpinWait();
        }

        if (size > 0) {
            LOGGER.info("Has dispatched {} logs", size);
        }

    }

    @Override
    public void destroy() {
        lock.lock();

        try {
            cleanUp();
            flush();
            waitForDispatch();

            subs.clear();
            if (disruptor != null) {
                disruptor.shutdown();
            }
        } finally {
            lock.unlock();
        }
    }

    protected void onFlush() {
    }

    protected abstract List<LogEntry> getBatch();

    protected abstract boolean shouldFlush();

    protected abstract void cleanUp();

}
