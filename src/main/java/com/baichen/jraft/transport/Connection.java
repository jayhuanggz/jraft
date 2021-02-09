package com.baichen.jraft.transport;

import com.baichen.jraft.Lifecycle;
import com.google.common.util.concurrent.ListenableFuture;

public interface Connection extends Lifecycle {

    ListenableFuture<Connection> connect();

    ListenableFuture<Connection> disconnect();


}
