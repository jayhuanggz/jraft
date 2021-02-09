package com.baichen.jraft.handler;

import com.baichen.jraft.model.RpcMessage;
import com.baichen.jraft.Server;
import com.baichen.jraft.transport.RpcResult;

public class MessageHandlerContextImpl implements MessageHandlerContext {

    private Server server;

    private RpcMessage message;

    private boolean stopped;

    private RpcResult result;

    public MessageHandlerContextImpl(Server server, RpcMessage message) {
        this.server = server;
        this.message = message;
    }

    @Override
    public Server getServer() {
        return server;
    }

    @Override
    public RpcMessage getMessage() {
        return message;
    }


    @Override
    public void release() {
        this.server = null;
        this.message = null;
    }

    @Override
    public void stop() {
        stopped = true;
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public RpcResult getResult() {
        return result;
    }

    @Override
    public void setResult(RpcResult result) {
        this.result = result;
        this.stopped = true;
    }
}
