package com.baichen.jraft.log;

import com.baichen.jraft.log.model.LogEntry;

public interface LogSerializer {

    LogEntry deserialize(int index, byte[] data);

    LogEntry deserialize(byte[] indexData, byte[] data);

    byte[] serialize(LogEntry log);
}
