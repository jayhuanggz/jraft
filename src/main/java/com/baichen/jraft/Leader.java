package com.baichen.jraft;

import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.model.AppendRequestSession;
import com.baichen.jraft.transport.RpcResult;

public interface Leader extends ServerStateInternal {

    void onAppendReplyReceived(RpcResult reply);

    void append(AppendRequestSession session);

    AppendRequestSession createAppendSession(byte[] command);

}
