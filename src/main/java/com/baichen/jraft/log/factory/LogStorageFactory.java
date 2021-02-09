package com.baichen.jraft.log.factory;

import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.options.LogStorageOptions;

public interface LogStorageFactory {

    LogStorage create(LogStorageOptions options);

    String getType();
}
