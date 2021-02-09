package com.baichen.jraft.log.rocksdb;

import com.baichen.jraft.log.model.LogEntry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.rocksdb.Options;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(MockitoJUnitRunner.class)
public class RocksDbLogStorageTest {

    private RocksDbLogStorage storage;

    private File dbFile;

    @Before
    public void init() throws IOException {

        Options options = new Options();
        String tempDir = System.getProperty("java.io.tmpdir");
        dbFile = new File(tempDir, RocksDbLogStorageTest.class.getName() + "." + UUID.randomUUID().toString());
        if (dbFile.exists()) {
            Files.walk(dbFile.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }

        dbFile.mkdir();

        storage = new RocksDbLogStorage(options, dbFile.getAbsolutePath());
        storage.start();
    }

    @After
    public void destroy() throws IOException {
        if (dbFile.exists()) {
            Files.walk(dbFile.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    public void testAppend_singleLog() throws InterruptedException {

        LogEntry log = new LogEntry(1, 1, new byte[0]);
        CountDownLatch latch = new CountDownLatch(1);

        storage.appendLog(log);

        Assert.assertEquals(log.getIndex(), log.getIndex());

        LogEntry lastLog = storage.getLastLog();
        Assert.assertEquals(log.getIndex(), lastLog.getIndex());

        LogEntry found = storage.findByIndex(log.getIndex());
        Assert.assertEquals(log.getIndex(), found.getIndex());


    }


    @Test
    public void testAppend_manyLogs() throws InterruptedException {

        int count = 100000;

        for (int i = 0; i < count; i++) {
            LogEntry log = new LogEntry(1, i + 1, new byte[0]);
            storage.appendLog(log);
        }


        LogEntry lastLog = storage.getLastLog();
        Assert.assertEquals(count, lastLog.getIndex());


        for (int i = 0; i < count; i++) {
            AtomicInteger index = new AtomicInteger(i + 1);
            LogEntry log = storage.findByIndex(i + 1);
            Assert.assertEquals(index.get(), log.getIndex());

        }


    }

    @Test
    public void testDeleteLogsStartingFrom() throws InterruptedException {

        int count = 100000;

        for (int i = 0; i < count; i++) {
            LogEntry log = new LogEntry(1, i + 1, new byte[0]);
            storage.appendLog(log);
        }

        storage.deleteLogsStartingFrom(2);
        LogEntry last = storage.getLastLog();
        Assert.assertEquals(1, last.getIndex());


    }

    @Test
    public void testFindByIndexStartingFrom() throws InterruptedException {

        int count = 100000;

        for (int i = 0; i < count; i++) {
            LogEntry log = new LogEntry(1, i + 1, new byte[0]);
            storage.appendLog(log);
        }
        List<LogEntry> logs = storage.findByIndexStartingFrom(5001);
        Assert.assertEquals(count - 5000, logs.size());
        //logs are in correct order
        for (int i = 5001; i < count; i++) {
            LogEntry log = logs.get(i - 5001);
            Assert.assertEquals(i, log.getIndex());
        }
    }

    @Test
    public void testFindByIndexStartingFrom_empty() throws InterruptedException {

        List<LogEntry> logs = storage.findByIndexStartingFrom(1);
        Assert.assertEquals(0, logs.size());
        int count = 1000;

        for (int i = 0; i < count; i++) {
            LogEntry log = new LogEntry(1, i + 1, new byte[0]);
            storage.appendLog(log);
        }
        logs = storage.findByIndexStartingFrom(count + 1);
        Assert.assertEquals(0, logs.size());
    }

    @Test
    public void testFindByIndexStartingFrom_useLastIndex() throws InterruptedException {

        int count = 1000;

        for (int i = 0; i < count; i++) {
            LogEntry log = new LogEntry(1, i + 1, new byte[0]);
            storage.appendLog(log);
        }
        List<LogEntry> logs = storage.findByIndexStartingFrom(count);

        Assert.assertEquals(1, logs.size());
        Assert.assertEquals(count, logs.get(0).getIndex());
    }

}
