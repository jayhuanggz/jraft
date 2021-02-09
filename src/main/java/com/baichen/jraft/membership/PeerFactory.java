package com.baichen.jraft.membership;

import com.baichen.jraft.transport.TransportFactory;
import com.baichen.jraft.value.NodeInfo;

public interface PeerFactory {

    Peer createPeer(NodeInfo nodeInfo, TransportFactory transportFactory);
}
