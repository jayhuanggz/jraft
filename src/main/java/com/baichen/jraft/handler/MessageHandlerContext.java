package com.baichen.jraft.handler;

import com.baichen.jraft.model.RpcMessage;
import com.baichen.jraft.Server;
import com.baichen.jraft.transport.RpcResult;

public interface MessageHandlerContext {

    Server getServer();

    RpcMessage getMessage();

    void release();

    void stop();

    boolean isStopped();

    RpcResult getResult();

    void setResult(RpcResult result);

}
