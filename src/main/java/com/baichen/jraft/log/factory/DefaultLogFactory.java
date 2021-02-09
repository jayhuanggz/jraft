package com.baichen.jraft.log.factory;

import com.baichen.jraft.membership.Peer;
import com.baichen.jraft.log.*;
import com.baichen.jraft.log.impl.DefaultLogBatcher;
import com.baichen.jraft.log.impl.LogCacheImpl;
import com.baichen.jraft.log.impl.LogManagerImpl;
import com.baichen.jraft.log.impl.LogStreamImpl;
import com.baichen.jraft.log.options.LogBatchOptions;
import com.baichen.jraft.log.options.LogCacheOptions;
import com.baichen.jraft.log.options.LogOptions;
import com.baichen.jraft.log.options.LogStorageOptions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DefaultLogFactory implements LogFactory {


    private Map<String, LogStorageFactory> storageFactoryMap = new HashMap<>();

    public DefaultLogFactory(Collection<LogStorageFactory> storageFactories) {

        assert storageFactories != null && !storageFactories.isEmpty();

        for (LogStorageFactory factory : storageFactories) {
            storageFactoryMap.put(factory.getType(), factory);
        }
    }

    @Override
    public void registerStorageFactory(LogStorageFactory storageFactory) {
        storageFactoryMap.put(storageFactory.getType(), storageFactory);
    }

    @Override
    public LogStorage createStorage(LogStorageOptions options) {

        LogStorageFactory factory = storageFactoryMap.get(options.getType());

        if (factory == null) {
            throw new IllegalArgumentException(String.format("LogStorageFactory for type %s is not found!", options.getType()));
        }
        return factory.create(options);
    }

    @Override
    public LogCache createCache(LogCacheOptions options) {
        return new LogCacheImpl(options.getInitialSize());
    }

    @Override
    public LogBatcher createBatcher(LogBatchOptions options) {
        return new DefaultLogBatcher(options.getBatchSize(), options.getFlushTimeout());
    }

    @Override
    public LogManager createManager(LogStorage storage, LogCache cache, LogBatcher batcher, LogOptions options) {
        return new LogManagerImpl(storage, cache, batcher, options);
    }

    @Override
    public LogStream createLogStream(Peer peer) {
        return new LogStreamImpl(peer);
    }

}
