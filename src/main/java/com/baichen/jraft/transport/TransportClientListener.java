package com.baichen.jraft.transport;

public interface TransportClientListener {

    void onStateChanged(TransportClient client, TransportClientState oldState, TransportClientState newState);


}
