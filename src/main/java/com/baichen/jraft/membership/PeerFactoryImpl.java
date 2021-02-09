package com.baichen.jraft.membership;

import com.baichen.jraft.transport.TransportFactory;
import com.baichen.jraft.value.NodeInfo;

public class PeerFactoryImpl implements PeerFactory {

    @Override
    public Peer createPeer(NodeInfo nodeInfo, TransportFactory transportFactory) {
        return new PeerImpl(nodeInfo, transportFactory);
    }
}
