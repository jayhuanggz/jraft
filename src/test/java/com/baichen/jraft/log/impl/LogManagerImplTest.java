package com.baichen.jraft.log.impl;

import com.baichen.jraft.log.LogBatcher;
import com.baichen.jraft.log.LogCache;
import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.SimpleLogBatcher;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.options.LogOptions;
import com.baichen.jraft.test.InMemoryLogStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(Parameterized.class)
public class LogManagerImplTest {

    private LogManagerImpl logManager;

    private LogStorage logStorage;

    private LogCache logCache;

    private LogBatcher batcher;

    @Parameterized.Parameters
    public static Object[][] data() {
        return new Object[10][0];
    }

    @Before
    public void init() {

        logStorage = new InMemoryLogStorage();
        logCache = new LogCacheImpl(1024);

        batcher = new SimpleLogBatcher(100);
        batcher.start();
        LogOptions options = new LogOptions();
        options.setDisruptorBufferSize(1024 * 1024);
        options.getCache().setMaxSizeHint(1024 * 1024);
        logManager = new LogManagerImpl(logStorage, logCache, batcher, options);

    }

    @After
    public void destroy() {
        batcher.destroy();
        logManager.destroy();
    }

    @Test
    public void testAppend() {
        logManager.start();

        logManager.append(new LogEntry(1, 1));

        LogEntry log = logManager.findByIndex(1);
        Assert.assertNotNull(log);
        Assert.assertEquals(1, log.getTerm());
        Assert.assertEquals(1, log.getIndex());

    }

    @Test
    public void testAppend_manyLogs() {
        logManager.start();

        int count = 100000;

        for (int i = 1; i <= count; i++) {
            logManager.append(new LogEntry(1, i));
        }
        for (int i = 1; i <= count; i++) {
            LogEntry log = logManager.findByIndex(i);
            Assert.assertEquals(i, log.getIndex());
        }
    }


    @Test
    public void testWhenCacheContainsNoLog() {
        int count = 100000;

        for (int i = 1; i <= count; i++) {
            logStorage.appendLog(new LogEntry(1, i));
        }
        logManager.start();

        Assert.assertEquals(count, logManager.getLastLogIndex());

        List<LogEntry> logs = logManager.findByIndexStartingFrom(1);

        Assert.assertEquals(100000, logs.size());
        for (int i = 1; i <= 100000; i++) {
            Assert.assertEquals(i, logs.get(i - 1).getIndex());
        }
    }

    @Test
    public void testWhenCacheContainsOnlyLatestLogs() {
        int count = 100000;

        // storage has 100000 logs
        for (int i = 1; i <= 50000; i++) {
            logStorage.appendLog(new LogEntry(1, i));
        }
        logManager.start();

        // cache contains logs 50001 to 100000
        for (int i = 50001; i <= count; i++) {
            logManager.append(new LogEntry(1, i));
        }

        Assert.assertEquals(count, logManager.getLastLogIndex());

        List<LogEntry> logs = logManager.findByIndexStartingFrom(1);

        Assert.assertEquals(count, logs.size());
        for (int i = 1; i <= count; i++) {
            Assert.assertEquals(i, logs.get(i - 1).getIndex());
        }
    }
}
