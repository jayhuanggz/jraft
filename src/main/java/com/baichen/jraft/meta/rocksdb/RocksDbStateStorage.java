package com.baichen.jraft.meta.rocksdb;

import com.baichen.jraft.exception.JRaftException;
import com.baichen.jraft.meta.PersistentState;
import com.baichen.jraft.meta.PersistentStateImpl;
import com.baichen.jraft.meta.StateStorage;
import com.baichen.jraft.util.ConvertUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.concurrent.atomic.AtomicInteger;

public class RocksDbStateStorage implements StateStorage {

    private static final byte[] NULL = new byte[0];

    private static final byte[] TERM_KEY = new byte[]{1};

    private static final byte[] VOTED_FOR_KEY = new byte[]{2};

    private static final byte[] VOTED_FOR_TERM_KEY = new byte[]{3};


    private static final int STATE_STOPPED = 0;

    private static final int STATE_STARTING = 1;

    private static final int STATE_READY = 2;

    private final Options options;

    private final String dbFile;

    private RocksDB db;

    private AtomicInteger state;

    private volatile PersistentStateImpl cache;

    public RocksDbStateStorage(Options options, String dbFile
    ) {
        this.options = options;
        this.dbFile = dbFile;
        this.state = new AtomicInteger(STATE_STOPPED);
    }


    private PersistentStateImpl loadStateFromDb() {
        int termVal = 0;

        try {
            byte[] term = db.get(TERM_KEY);
            if (term != null) {
                termVal = ConvertUtils.bytesToInt(term);
            }
        } catch (RocksDBException e) {
        }
        String voteFor = null;

        try {
            byte[] vote = db.get(VOTED_FOR_KEY);
            if (vote != null) {
                voteFor = vote.length == 0 ? null : new String(vote);
            }
        } catch (RocksDBException e) {
        }
        int votedForTermVal = 0;
        try {
            byte[] votedForTerm = db.get(VOTED_FOR_TERM_KEY);
            if (votedForTerm != null) {
                votedForTermVal = ConvertUtils.bytesToInt(votedForTerm);
            }
        } catch (RocksDBException e) {
        }

        return new PersistentStateImpl(termVal, voteFor, votedForTermVal);
    }

    private void checkIsReady() {
        if (state.get() != STATE_READY) {
            throw new IllegalStateException("RocksDbStateStorage is not in READY state!");
        }
    }

    @Override
    public PersistentState getState() {
        checkIsReady();
        return cache;
    }

    @Override
    public void updateTerm(int term) {
        checkIsReady();

        try {
            db.put(TERM_KEY, ConvertUtils.intToBytes(term));
            cache.setTerm(term);
        } catch (RocksDBException e) {

        }

    }

    @Override
    public int incrementTerm() {
        updateTerm(cache.getTerm() + 1);
        return cache.getTerm();
    }

    @Override
    public void updateVoteFor(String candidateId, int term) {
        checkIsReady();

        String oldValue = cache.getVotedFor();

        try {
            cache.setVotedFor(candidateId);
            cache.setVotedForTerm(term);
            byte[] value = candidateId == null ? NULL : candidateId.getBytes();
            db.put(VOTED_FOR_KEY, value);
            db.put(VOTED_FOR_TERM_KEY, ConvertUtils.intToBytes(term));
        } catch (RocksDBException e) {
            cache.setVotedFor(oldValue);
        }

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
            cache = loadStateFromDb();
            state.set(STATE_READY);
        }

    }

    @Override
    public void destroy() {


        if (state.get() != STATE_STOPPED) {

            synchronized (this) {
                try {
                    if (db != null) {
                        db.close();
                    }

                } finally {
                    state.set(STATE_STOPPED);
                }
            }

        }


    }
}
