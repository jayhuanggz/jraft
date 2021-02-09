package com.baichen.jraft.log.impl;

import com.baichen.jraft.log.LogBatcher;
import com.baichen.jraft.log.LogCache;
import com.baichen.jraft.log.LogManager;
import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.options.LogOptions;
import com.baichen.jraft.util.NamedThreadFactory;
import com.baichen.jraft.util.Subscription;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.ThreadHints;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LogManagerImpl implements LogManager {

    private static final Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(LogManagerImpl.class);

    private static final int STATE_NOT_STARTED = 0;

    private static final int STATE_STARTING = 1;

    private static final int STATE_READY = 2;

    private static final int STATE_DESTROYED = 3;

    private final ConcurrentHashMap<Integer, LogEvent> inflightEvents;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Lock readLock = lock.readLock();

    private final Lock writeLock = lock.writeLock();

    private final LogStorage logStorage;

    private final LogOptions options;

    private final AtomicInteger state;

    private final LogCache cache;

    private final AtomicInteger requestId = new AtomicInteger(1);

    // keep track of the last log index persisted since the server starts up
    // it is different from the last log index in storage as storage modifications (append & delete)
    // are executed async, while this field records the most up to date value.
    // For example, if there is a log conflict, logs will be deleted from cache immediately,
    // but the actual deletion from storage is queued by Disruptor and executed later in an async manner.
    // Any immediate subsequent query should behave like the logs have been deleted from storage
    private volatile int lastPersistedLogIndex = 0;

    private int cacheMaxSizeHint;

    private Disruptor<LogEvent> disruptor;

    private RingBuffer<LogEvent> ringBuffer;

    // last index in cache
    private volatile int lastIndex = 0;

    private final EventTranslatorVararg<LogEvent> translator;

    private LogBatcher logBatcher;

    private Subscription batchSubscription;


    public LogManagerImpl(LogStorage logStorage, LogCache cache,
                          LogBatcher logBatcher,
                          LogOptions options) {
        this.logStorage = logStorage;
        this.options = options;
        this.state = new AtomicInteger(STATE_NOT_STARTED);
        this.cache = cache;
        this.inflightEvents = new ConcurrentHashMap<>(1024);
        translator = new EventTranslatorVararg<LogEvent>() {
            @Override
            public void translateTo(LogEvent event, long sequence, Object... args) {
                event.clear();
                int requestId = LogManagerImpl.this.requestId.getAndIncrement();
                event.setRequestId(requestId);
                event.setLogs((List<LogEntry>) args[0]);
                event.setIndex((int) args[1]);
                inflightEvents.put(requestId, event);


            }
        };
        this.logBatcher = logBatcher;
        this.batchSubscription = this.logBatcher.subscribe(this::onBatchFlush);
        this.cacheMaxSizeHint = options.getCache().getMaxSizeHint();

    }

    private void onBatchFlush(Collection<LogEntry> logs) {

        publishEvent(logs, 0);
    }


    private void checkIsReady() {
        if (state.get() != STATE_READY) {

            if (state.get() == STATE_DESTROYED) {
                throw new IllegalStateException("LogManager has destroyed!");
            } else {
                throw new IllegalStateException("LogManager in not READY!");
            }

        }
    }


    private void doLogStorageAppend(List<LogEntry> logs) {


        // Only persist if the log exists in cache, for the case
        // when incoming logs has been deleted from cache(e.g due to log conflict).
        // If the last log exists in cache, then all the incoming logs
        // must be in the cache as well

        LOGGER.debug("doLogStorageAppend for {} logs ", logs.size());

        LogEntry lastLog = logs.get(logs.size() - 1);
        LogEntry logFromCache = cache.findByIndex(lastLog.getIndex());
        if (logFromCache != null && logFromCache.getTerm() == lastLog.getTerm()) {
            List<LogEntry> filtered = new ArrayList<>(logs.size());

            for (LogEntry log : logs) {
                if (log.getIndex() > lastPersistedLogIndex) {
                    filtered.add(log);
                }
            }

            if (filtered.isEmpty()) {
                return;
            }

            if (filtered.size() == 1) {
                LogEntry log = filtered.get(0);
                LOGGER.debug("Store log index {}, term {}", log.getIndex(), log.getTerm());
                try {
                    logStorage.appendLog(log);
                    lastPersistedLogIndex = log.getIndex();
                } catch (Throwable e) {
                    LOGGER.warn("Something went wrong while store log, index: "
                            + log.getIndex() + ", term: " + log.getTerm(), e);
                    lastPersistedLogIndex = logStorage.getLastLogIndex();
                }
            } else {
                try {
                    logStorage.batchAppend(filtered);
                    lastPersistedLogIndex = filtered.get(filtered.size() - 1).getIndex();
                } catch (Throwable e) {
                    LOGGER.warn("Something went wrong while store logs in batch, logs: " + filtered.size(), e);
                    lastPersistedLogIndex = logStorage.getLastLogIndex();
                }
            }
            tryShrinkCacheIfNeeded();
        }


    }


    private void tryShrinkCacheIfNeeded() {

        if (cacheMaxSizeHint <= cache.size()) {

            if (lastPersistedLogIndex > 0) {
                writeLock.lock();
                try {
                    // only delete logs from cache that have been persisted
                    int deleted = cache.deleteLogsBefore(lastPersistedLogIndex);
                    if (deleted > 0) {
                        LogEntry lastLogFromStorage = logStorage.getLastLog();
                        if (cache.size() == 0) {
                            if (lastLogFromStorage == null) {
                                lastIndex = 0;
                            } else {
                                lastIndex = lastLogFromStorage.getIndex();
                                cache.append(lastLogFromStorage);
                            }
                        }
                    }
                } finally {
                    writeLock.unlock();
                }
            }


        }
    }

    @Override
    public void append(LogEntry log) {
        checkIsReady();
        writeLock.lock();
        try {
            cache.append(log);
            lastIndex = log.getIndex();
        } finally {
            writeLock.unlock();
        }
        logBatcher.add(log);

    }


    private void publishEvent(Object... args) {
        while (!ringBuffer.tryPublishEvent(translator, args)) {
            ThreadHints.onSpinWait();
        }
    }

    @Override
    public void append(List<LogEntry> logs) {

        checkIsReady();
        writeLock.lock();
        try {
            for (LogEntry log : logs) {
                cache.append(log);
                lastIndex = log.getIndex();
            }

        } finally {
            writeLock.unlock();
        }
        logBatcher.add(logs);


    }


    @Override
    public LogEntry findByIndex(int index) {
        checkIsReady();

        readLock.lock();

        try {

            LogEntry log = cache.findByIndex(index);
            if (log == null) {
                log = logStorage.findByIndex(index);
            }
            return log;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<LogEntry> findByIndexStartingFrom(int startIndex) {

        readLock.lock();
        try {

            if (lastIndex == 0) {
                return Collections.emptyList();
            }

            LogEntry[] latestLogsFromCache = cache.findByIndexStartingFrom(startIndex);

            int firstLogIndexFromCache = 0;

            if (latestLogsFromCache.length > 0) {
                LogEntry firstLog = latestLogsFromCache[0];
                if (firstLog.getIndex() == startIndex) {
                    return Arrays.asList(latestLogsFromCache);
                }
                firstLogIndexFromCache = firstLog.getIndex();
            }


            final List<LogEntry> persistedLogs;

            int lastPersistedLogIndexCopy = lastPersistedLogIndex;

            if (lastPersistedLogIndexCopy >= startIndex) {
                persistedLogs = logStorage.findByIndexInRange(startIndex, firstLogIndexFromCache > 1 ? firstLogIndexFromCache - 1 : lastPersistedLogIndexCopy);
            } else {
                persistedLogs = Collections.emptyList();
            }


            List<LogEntry> result = new ArrayList<>(latestLogsFromCache.length + persistedLogs.size());
            result.addAll(persistedLogs);
            for (LogEntry log : latestLogsFromCache) {
                result.add(log);
            }
            return result;

        } finally {
            readLock.unlock();
        }
    }


    @Override
    public int getLastLogTerm() {
        readLock.lock();

        try {
            LogEntry lastLog = cache.getLastLog();

            return lastLog == null ? 0 : lastLog.getTerm();
        } finally {
            readLock.unlock();
        }
    }


    @Override
    public int getLastLogIndex() {
        readLock.lock();

        try {
            LogEntry lastLog = cache.getLastLog();


            return lastLog == null ? 0 : lastLog.getIndex();
        } finally {
            readLock.unlock();
        }
    }


    public LogEntry getLastLog() {
        readLock.lock();

        try {
            return cache.getLastLog();

        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void start() {

        if (state.compareAndSet(STATE_NOT_STARTED, STATE_STARTING)) {
            disruptor = new Disruptor<>(new LogEventFactory(),
                    options.getDisruptorBufferSize(),
                    new NamedThreadFactory("JRaft-LogManagerImpl", true),
                    ProducerType.SINGLE, new BusySpinWaitStrategy());

            disruptor.handleEventsWith(new LogEventHandler(this));
            ringBuffer = disruptor.start();
            LogEntry lastLog = logStorage.getLastLog();
            if (lastLog != null) {
                cache.append(lastLog);
                lastIndex = lastLog.getIndex();
                lastPersistedLogIndex = lastLog.getIndex();
            }

            state.set(STATE_READY);
        }

    }


    private void waitForInflightEvents() {
        int count = 0;
        int size = inflightEvents.size();
        if (size > 0) {
            LOGGER.info("Has {} inflight events, wait for completion before shutting down...... ", size);
        }
        int sleepCount = 200;

        while (!inflightEvents.isEmpty()) {

            if (++count >= sleepCount) {

                try {
                    LOGGER.info("It is taking too long for consuming inflight events, use sleep instead of busy spin......");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            } else {
                ThreadHints.onSpinWait();
            }
        }
    }


    @Override
    public void destroy() {
        int val;
        LOGGER.info("Shutting down LogManager gracefully......");
        boolean destroyedPreviously = true;

        while ((val = state.get()) != STATE_DESTROYED) {

            if (state.compareAndSet(val, STATE_DESTROYED)) {
                destroyedPreviously = false;
                waitForInflightEvents();

                if (batchSubscription != null) {
                    batchSubscription.cancel();
                    batchSubscription = null;
                }
                if (disruptor != null) {
                    disruptor.shutdown();
                    disruptor = null;
                }
            }
        }
        if (destroyedPreviously) {
            LOGGER.warn("LogManager has been destroyed previously, duplicate call of destroy() is " +
                    " an indication of bug, ignore for now......");
        } else {
            LOGGER.info("LogManager has shut down gracefully......");
        }
    }


    private static final class LogEventHandler implements EventHandler<LogEvent> {

        private WeakReference<LogManagerImpl> manager;

        public LogEventHandler(LogManagerImpl manager) {
            this.manager = new WeakReference<>(manager);
        }

        @Override
        public void onEvent(LogEvent event, long l, boolean b) throws Exception {

            LogManagerImpl manager = this.manager.get();
            if (manager != null) {
                try {
                    manager.doLogStorageAppend(event.getLogs());
                } catch (Throwable e) {
                    LOGGER.warn(e.getMessage(), e);
                } finally {
                    manager.inflightEvents.remove(event.getRequestId());
                }
            }

        }


    }


    private static final class LogEventFactory implements EventFactory<LogEvent> {

        @Override
        public LogEvent newInstance() {
            return new LogEvent();
        }
    }


    private static final class LogEvent implements Serializable {

        private int requestId;

        private List<LogEntry> logs;

        private int index;

        public LogEvent() {
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public int getRequestId() {
            return requestId;
        }

        public void setRequestId(int requestId) {
            this.requestId = requestId;
        }

        public List<LogEntry> getLogs() {
            return logs;
        }

        public void setLogs(List<LogEntry> logs) {
            this.logs = logs;
        }

        private void clear() {
            logs = null;
        }
    }


}
