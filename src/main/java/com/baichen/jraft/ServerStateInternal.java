package com.baichen.jraft;

import com.baichen.jraft.log.model.LogEntry;

public interface ServerStateInternal extends Lifecycle {

    LogEntry getLastLog();

    int getLastLogIndex();

    int getLastLogTerm();


}
