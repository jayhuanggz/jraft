package com.baichen.jraft.transport;

import com.baichen.jraft.value.NodeInfo;

public interface TransportFactory {

    TransportServer createServer(TransportOptions options, NodeInfo node);

    TransportClient createClient(TransportOptions options, NodeInfo node);

    String getType();
}
