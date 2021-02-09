package com.baichen.jraft.impl;

import com.baichen.jraft.AppendRequestSessionListener;
import com.baichen.jraft.AppendRequestSessionManager;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.value.LogEntryId;
import com.baichen.jraft.model.AppendRequestSession;
import com.baichen.jraft.model.AppendRequestSessionImpl;
import com.baichen.jraft.util.RepeatableHashedWheelTimer;
import com.baichen.jraft.util.RepeatableTimer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AppendRequestSessionManagerImpl implements AppendRequestSessionManager {

    private static final Logger LOGGER = LogManager.getLogger(AppendRequestSessionManagerImpl.class);

    private final ConcurrentHashMap<LogEntryId, Entry> sessions = new ConcurrentHashMap<>(1024);

    private RepeatableHashedWheelTimer timer;

    private volatile int minPendingRequestIndex = 0;

    private final AtomicInteger sessionId = new AtomicInteger(0);

    @Override
    public void startSession(AppendRequestSession session, LogEntry log, long timeout,
                             AppendRequestSessionListener listener) {


        LogEntryId logId = new LogEntryId(log.getTerm(), log.getIndex());
        Entry entry = sessions.get(logId);

        if (entry != null) {
            LOGGER.info("Session already created: entry term {}, entry index {}, " +
                            "incoming log term {}, incoming log index {}",
                    entry.session.getLog().getTerm(), entry.session.getLog().getIndex(), log.getTerm(), log
                            .getIndex());
            return;
        }


        final Entry newEntry = new Entry();
        newEntry.session = session;
        newEntry.listener = listener;

        Entry prev = sessions.putIfAbsent(logId, newEntry);

        if (prev != null) {
            LOGGER.info("Session already created: entry term {}, entry index {}, incoming log term {}, " +
                            "incoming log index {}",
                    prev.session.getLog().getTerm(), prev.session.getLog().getIndex(), log.getTerm(), log
                            .getIndex());
        }

        session.setLog(log);

        newEntry.timeout = timer.schedule(() -> {
            AppendRequestSession sessionCopy = newEntry.session;
            if (sessionCopy == null) {
                return;
            }
            Entry existing = null;
            synchronized (sessions) {
                existing = sessions.remove(new LogEntryId(sessionCopy.getLog().getTerm(), sessionCopy.getLog().getIndex()));

            }
            if (existing != null) {
                LOGGER.info("Append Session timeout, term {}, index {}", sessionCopy.getLog().getTerm(),
                        sessionCopy.getLog().getIndex());
                try {
                    sessionCopy.cancel();
                    existing.listener.onTimeout(sessionCopy);


                } finally {
                    existing.destroy();
                }
            }

        }, () -> timeout);

        if (minPendingRequestIndex == 0) {
            minPendingRequestIndex = log.getIndex();
        }

    }

    @Override
    public AppendRequestSession createSession(byte[] command) {
        return new AppendRequestSessionImpl(sessionId.incrementAndGet(), command);
    }

    @Override
    public AppendRequestSession findSession(LogEntryId logId) {
        Entry entry = sessions.get(logId);
        return entry == null ? null : entry.session;
    }


    @Override
    public void commitSession(LogEntryId id) {
        if (sessions.isEmpty()) {
            return;
        }

        //Commit the session matching the committed index
        Entry entry = null;

        synchronized (sessions) {
            entry = sessions.remove(id);
        }
        if (entry != null) {
            entry.session.complete();
            try {
                entry.listener.onCommitted(entry.session);
            } finally {
                entry.destroy();
            }
        }


        // commit sessions with log index lower than the committed index, which means
        // committing older logs that are still waiting

        int minPendingRequestIndexCopy = this.minPendingRequestIndex;

        if (minPendingRequestIndexCopy > 0) {
            LogEntryId tempId = new LogEntryId(id.getTerm(), id.getIndex());
            for (int i = id.getIndex() - 1; i >= minPendingRequestIndexCopy && !sessions.isEmpty(); i--) {
                tempId.setIndex(i);
                synchronized (sessions) {
                    entry = sessions.remove(tempId);
                }
                if (entry != null) {
                    entry.session.complete();
                    try {
                        entry.listener.onCommitted(entry.session);
                    } finally {
                        entry.destroy();
                    }

                }

                if (i == minPendingRequestIndexCopy) {
                    this.minPendingRequestIndex = id.getIndex() + 1;
                }
            }


        }

        synchronized (sessions) {
            if (sessions.isEmpty()) {
                minPendingRequestIndex = id.getIndex() + 1;
            }
        }
    }


    @Override
    public void start() {
        timer = new RepeatableHashedWheelTimer(20, 50, TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() {

        timer.shutdown();

        for (Entry entry : sessions.values()) {
            try {
                entry.session.cancel();
            } finally {
                entry.destroy();
            }
        }

        sessions.clear();
    }

    private static final class Entry {

        private AppendRequestSession session;

        private RepeatableTimer.TimerTask timeout;

        private AppendRequestSessionListener listener;

        void destroy() {

            if (timeout != null) {
                timeout.cancel();

            }

            session = null;
            timeout = null;
            listener = null;
        }
    }
}
