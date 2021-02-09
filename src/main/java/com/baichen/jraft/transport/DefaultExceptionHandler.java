package com.baichen.jraft.transport;

import com.baichen.jraft.value.NodeInfo;

public class DefaultExceptionHandler implements ExceptionHandler {
    @Override
    public RpcResult handle(int requestId, NodeInfo nodeInfo, Throwable e) {

        RpcResult result = new RpcResult();
        result.setRequestId(requestId);
        result.setMsg(e.getMessage());
        result.setCode(RpcResult.Codes.UNKNOWN);
        result.setNodeId(nodeInfo.getId());
        return result;

    }
}
