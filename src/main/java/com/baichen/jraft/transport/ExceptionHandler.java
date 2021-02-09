package com.baichen.jraft.transport;

import com.baichen.jraft.value.NodeInfo;

public interface ExceptionHandler {

    RpcResult handle(int requestId, NodeInfo nodeInfo,Throwable e);
}
