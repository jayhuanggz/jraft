package com.baichen.jraft;

import com.baichen.jraft.impl.CandidateImplTest;
import com.baichen.jraft.impl.FollowerImplTest;
import com.baichen.jraft.impl.LeaderImplTest;
import com.baichen.jraft.log.impl.LogCacheImplTest;
import com.baichen.jraft.log.impl.LogStreamImplTest;
import com.baichen.jraft.log.impl.ReplicatorImplTest;
import com.baichen.jraft.log.rocksdb.RocksDbLogStorageTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        CandidateImplTest.class,
        FollowerImplTest.class,
        LeaderImplTest.class,
        LogStreamImplTest.class,
        ReplicatorImplTest.class,
        RocksDbLogStorageTest.class,
        RocksDbStateStorageTest.class,
        LogCacheImplTest.class
})
public class JRaftTestSuite {
}
