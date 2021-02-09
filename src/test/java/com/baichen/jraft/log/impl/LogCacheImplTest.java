package com.baichen.jraft.log.impl;

import com.baichen.jraft.log.impl.LogCacheImpl;
import com.baichen.jraft.log.model.LogEntry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LogCacheImplTest {

    private LogCacheImpl cache;

    @Before
    public void init() {
        cache = new LogCacheImpl(1024);

    }

    @Test
    public void testAppend_emptyCache() {
        cache.append(new LogEntry(1, 1));
        Assert.assertEquals(1, cache.size());
        LogEntry lastLog = cache.getLastLog();
        Assert.assertEquals(1, lastLog.getIndex());
        Assert.assertEquals(1, lastLog.getTerm());

        cache.append(new LogEntry(1, 2));
        Assert.assertEquals(2, cache.size());
        lastLog = cache.getLastLog();
        Assert.assertEquals(2, lastLog.getIndex());
        Assert.assertEquals(1, lastLog.getTerm());

    }

    @Test
    public void testAppend_manyLogs() {

        int count = 100000;
        for (int i = 0; i < count; i++) {
            cache.append(new LogEntry(1, i + 1));
        }

        Assert.assertEquals(count, cache.size());
        LogEntry lastLog = cache.getLastLog();
        Assert.assertEquals(count, lastLog.getIndex());
        Assert.assertEquals(1, lastLog.getTerm());

        for (int i = 0; i < count; i++) {
            LogEntry log = cache.findByIndex(i + 1);
            Assert.assertEquals(i + 1, log.getIndex());

        }
    }

    @Test
    public void testFindByIndex_empty() {
        LogEntry log = cache.findByIndex(0);
        Assert.assertNull(log);
    }

    @Test
    public void testFindByIndex_onlyOneElement() {
        cache.append(new LogEntry(1, 10));

        LogEntry log = cache.findByIndex(10);
        Assert.assertEquals(10, log.getIndex());
        Assert.assertEquals(1, log.getTerm());
    }

    @Test
    public void testFindByIndex_notFound() {
        int count = 100000;
        for (int i = 0; i < count; i++) {
            cache.append(new LogEntry(1, i + 1));
        }

        LogEntry log = cache.findByIndex(count + 1);
        Assert.assertNull(log);

    }

    @Test
    public void testDeleteLogsStartingFrom() {
        int count = 100000;
        for (int i = 0; i < count; i++) {
            cache.append(new LogEntry(1, i + 1));
        }
        int deleted = cache.deleteLogsStartingFrom(50001);
        Assert.assertEquals(50000, deleted);
        Assert.assertEquals(count - 50000, cache.size());

        for (int i = 0; i < count; i++) {
            LogEntry log = cache.findByIndex(i + 1);
            if (i <= 49999) {
                Assert.assertNotNull(log);
            } else {
                Assert.assertNull(log);
            }
        }
    }


    @Test
    public void testDeleteLogsStartingFrom_startIndexNotFound() {
        int count = 100000;
        for (int i = 0; i < count; i++) {
            // missing index from 50001 to 60000
            if (i < 50000 || i > 59999) {
                cache.append(new LogEntry(1, i + 1));
            }

        }

        Assert.assertNull(cache.findByIndex(50001));

        int deleted = cache.deleteLogsStartingFrom(50001);
        Assert.assertEquals(40000, deleted);
        Assert.assertEquals(50000, cache.size());

        for (int i = 0; i < count; i++) {
            LogEntry log = cache.findByIndex(i + 1);
            if (i < 50000) {
                Assert.assertNotNull(log);
            } else {
                Assert.assertNull(log);
            }
        }
    }

    @Test
    public void testFindByIndexStartingFrom() {
        int count = 100000;
        for (int i = 0; i < count; i++) {
            cache.append(new LogEntry(1, i + 1));
        }

        LogEntry[] logs = cache.findByIndexStartingFrom(50001);
        Assert.assertEquals(50000, logs.length);

        for (int i = 0; i < logs.length; i++) {
            LogEntry log = logs[i];
            Assert.assertEquals(i + logs.length + 1, log.getIndex());

        }
    }

    @Test
    public void testFindByIndexStartingFrom_notFound() {

        LogEntry[] logs = cache.findByIndexStartingFrom(1);
        Assert.assertEquals(0, logs.length);

    }


    @Test
    public void testDeleteLogsBefore_empty() {

        int deleted = cache.deleteLogsBefore(10);
        Assert.assertEquals(0, deleted);


    }

    @Test
    public void testDeleteLogsBefore_deleteAll() {

        int count = 100000;
        for (int i = 0; i < count; i++) {
            cache.append(new LogEntry(1, i + 1));
        }

        int deleted = cache.deleteLogsBefore(count);
        Assert.assertEquals(count, deleted);
        Assert.assertEquals(0, cache.size());


    }


    @Test
    public void testDeleteLogsBefore_deleteSome() {

        int count = 100000;
        for (int i = 0; i < count; i++) {
            cache.append(new LogEntry(1, i + 1));
        }

        int deleted = cache.deleteLogsBefore(40000);
        Assert.assertEquals(40000, deleted);
        Assert.assertEquals(60000, cache.size());

        LogEntry[] logs = cache.findByIndexStartingFrom(40001);
        for (int i = 0; i < logs.length; i++) {
            LogEntry log = logs[i];
            Assert.assertEquals(i + 40001, log.getIndex());

        }

    }

}
