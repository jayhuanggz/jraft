package com.baichen.jraft.log.factory;

import com.baichen.jraft.membership.Peer;
import com.baichen.jraft.log.*;
import com.baichen.jraft.log.options.LogBatchOptions;
import com.baichen.jraft.log.options.LogCacheOptions;
import com.baichen.jraft.log.options.LogOptions;
import com.baichen.jraft.log.options.LogStorageOptions;

public interface LogFactory {

    void registerStorageFactory(LogStorageFactory storageFactory);

    LogStorage createStorage(LogStorageOptions options);

    LogCache createCache(LogCacheOptions options);

    LogBatcher createBatcher(LogBatchOptions options);

    LogManager createManager(LogStorage storage, LogCache cache, LogBatcher batcher, LogOptions options);

    LogStream createLogStream(Peer peer);


}
