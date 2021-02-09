package com.baichen.jraft;

import com.baichen.jraft.meta.PersistentState;
import com.baichen.jraft.meta.rocksdb.RocksDbStateStorage;
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
import java.util.UUID;

@RunWith(MockitoJUnitRunner.class)
public class RocksDbStateStorageTest {

    private RocksDbStateStorage storage;

    private File dbFile;

    @Before
    public void init() throws IOException {

        Options options = new Options();
        String tempDir = System.getProperty("java.io.tmpdir");
        dbFile = new File(tempDir, RocksDbStateStorageTest.class.getName() + "." + UUID.randomUUID().toString());
        if (dbFile.exists()) {
            Files.walk(dbFile.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }

        dbFile.mkdir();
        storage = new RocksDbStateStorage(options, dbFile.getAbsolutePath());
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
    public void testGetState_noData() {
        PersistentState state = storage.getState();
        Assert.assertNotNull(state);

        Assert.assertEquals(0, state.getTerm());
        Assert.assertNull(state.getVotedFor());

    }

    @Test
    public void testIncrementTerm() {
        PersistentState state = storage.getState();

        Assert.assertEquals(0, state.getTerm());
        int term = storage.incrementTerm();

        Assert.assertEquals(1, term);

        state = storage.getState();

        Assert.assertEquals(1, state.getTerm());


    }

    @Test
    public void testUpdateVotedFor() {
        PersistentState state = storage.getState();

        Assert.assertNull(state.getVotedFor());

        String candidateId = "candidate1";
        storage.updateVoteFor(candidateId, 1);

        state = storage.getState();

        Assert.assertEquals(candidateId, state.getVotedFor());
        Assert.assertEquals(1, state.getVotedForTerm());

    }


}
