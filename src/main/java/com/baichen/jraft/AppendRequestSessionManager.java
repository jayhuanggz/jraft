package com.baichen.jraft;

import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.value.LogEntryId;
import com.baichen.jraft.model.AppendRequestSession;

/**
 * Manages append request sessions:
 * 1. handles session timeout
 * 2. manages session lifecycles and lookups
 */
public interface AppendRequestSessionManager extends Lifecycle {

    void startSession(AppendRequestSession session, LogEntry log, long timeout, AppendRequestSessionListener listener);

    AppendRequestSession createSession(byte[] command);

    AppendRequestSession findSession(LogEntryId logId);

    void commitSession(LogEntryId id);


}
