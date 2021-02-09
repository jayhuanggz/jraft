package com.baichen.jraft.transport;


import com.baichen.jraft.Lifecycle;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A rpc server that receives messages from peers
 */
public interface TransportServer extends Lifecycle {

    void registerService(TransportServerService service);

    TransportServerState getState();

    ListenableFuture<Void> startWithFuture();

}
