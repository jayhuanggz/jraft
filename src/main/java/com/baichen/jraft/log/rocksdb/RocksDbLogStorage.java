package com.baichen.jraft.log.rocksdb;

import com.baichen.jraft.exception.JRaftException;
import com.baichen.jraft.log.LogSerializer;
import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.impl.DefaultLogSerializer;
import com.baichen.jraft.log.model.LogEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class RocksDbLogStorage implements LogStorage {

    private static final int STATE_STOPPED = 0;

    private static final int STATE_STARTING = 1;

    private static final int STATE_READY = 2;


    private static final Logger LOGGER = LogManager.getLogger(RocksDbLogStorage.class.getName());

    static {
        RocksDB.loadLibrary();
    }

    private final Options options;

    private final String dbFile;

    private RocksDB db;

    private LogSerializer logSerializer = new DefaultLogSerializer();

    private AtomicInteger state;

    private volatile LogEntry lastLog;


    public RocksDbLogStorage(Options options, String dbFile) {
        this.options = options;
        this.dbFile = dbFile;
        this.state = new AtomicInteger(STATE_STOPPED);
    }

    @Override
    public void appendLog(LogEntry log) {
        checkIsReady();
        LOGGER.debug("Appending LogEntry index: {}, term: {}", log.getIndex(), log.getTerm());
        try {
            db.put(getDbId(log.getIndex()), logSerializer.serialize(log));
            lastLog = log;
        } catch (RocksDBException e) {
            LOGGER.warn("Failed to put LogEntry index: {}, term: {}, {}",
                    log.getIndex(), log.getTerm(), e.getMessage());

            throw new JRaftException(e);
        }

    }

    @Override
    public void batchAppend(List<LogEntry> logs) {

        WriteBatch writeBatch = new WriteBatch();

        for (LogEntry log : logs) {
            try {
                writeBatch.put(getDbId(log.getIndex()), logSerializer.serialize(log));
            } catch (RocksDBException e) {
                throw new JRaftException(e);
            }

        }

        try {
            db.write(new WriteOptions(), writeBatch);
            if (!logs.isEmpty()) {
                lastLog = logs.get(logs.size() - 1);
            }
        } catch (RocksDBException e) {
            throw new JRaftException(e);
        }
    }

    @Override
    public LogEntry getLastLog() {
        checkIsReady();
        return findLastLog();
    }


    @Override
    public LogEntry findByIndex(int index) {

        checkIsReady();

        try {
            byte[] data = db.get(getDbId(index));
            if (data == null) {
                return null;
            }
            return logSerializer.deserialize(index, data);
        } catch (RocksDBException e) {
            return null;
        }


    }

    @Override
    public int deleteLogsStartingFrom(int startIndex) {
        checkIsReady();

        if (lastLog == null) {
            return 0;
        }
        int lastIndex = lastLog.getIndex();

        int deleted = 0;

        try {

            WriteBatch batch = new WriteBatch();
            for (int i = startIndex; i <= lastIndex; i++) {
                batch.delete(getDbId(i));
                deleted++;
            }
            db.write(new WriteOptions(), batch);

            lastLog = findLastLog();

        } catch (RocksDBException e) {
            LOGGER.warn("Error deleting logs from {} to {}, {}", startIndex, lastIndex, e.getMessage());
            return 0;
        }
        return deleted;
    }

    @Override
    public List<LogEntry> findByIndexStartingFrom(int startIndex) {


        try {
            RocksIterator it = db.newIterator();
            it.seek(getDbId(startIndex));

            List<LogEntry> logs = new LinkedList<>();
            while (it.isValid()) {
                LogEntry log = readLog(it);
                if (log == null) {
                    break;
                }
                logs.add(log);
                it.next();
            }
            return logs;
        } catch (Throwable e) {
            return Collections.emptyList();
        }


    }

    @Override
    public List<LogEntry> findByIndexInRange(int startIndex, int endIndex) {


        try {
            RocksIterator it = db.newIterator();
            it.seek(getDbId(startIndex));

            int index = startIndex;
            List<LogEntry> logs = new LinkedList<>();
            while (it.isValid() && index <= endIndex) {
                LogEntry log = readLog(it);
                if (log == null) {
                    break;
                }
                logs.add(log);
                it.next();
                index++;
            }
            return logs;
        } catch (Throwable e) {
            return Collections.emptyList();
        }


    }

    @Override
    public int getLastLogTerm() {

        return lastLog == null ? 0 : lastLog.getTerm();
    }

    @Override
    public int getLastLogIndex() {
        return lastLog == null ? 0 : lastLog.getIndex();
    }

    private void checkIsReady() {
        if (state.get() != STATE_READY) {
            throw new IllegalStateException("RocksDbLogStorage is not in READY state!");
        }
    }

    private LogEntry findLastLog() {
        RocksIterator it = db.newIterator();
        it.seekToLast();
        return readLog(it);
    }

    private LogEntry readLog(RocksIterator it) {

        if (!it.isValid()) {
            return null;
        }
        byte[] lastIndex = it.key();

        if (lastIndex != null) {
            byte[] value = it.value();
            return logSerializer.deserialize(lastIndex, value);
        }
        return null;
    }


    private byte[] getDbId(int index) {
        return ByteBuffer.allocate(4).putInt(index).array();
    }


    @Override
    public void start() {

        if (state.compareAndSet(STATE_STOPPED, STATE_STARTING)) {
            try {
                options.setCreateIfMissing(true);
                db = RocksDB.open(options, dbFile);
            } catch (RocksDBException e) {
                throw new JRaftException(e);
            }

            lastLog = findLastLog();
            state.set(STATE_READY);
        }


    }

    @Override
    public void destroy() {


        if (state.get() != STATE_STOPPED) {

            synchronized (this) {
                try {
                    if (db != null) {
                        try {
                            db.flushWal(true);
                        } catch (RocksDBException e) {
                            LOGGER.warn("Error calling flushWal(true)", e);
                        }
                        db.close();
                    }


                } finally {
                    state.set(STATE_STOPPED);
                }
            }

        }


    }
}
