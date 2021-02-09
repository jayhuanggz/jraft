package com.baichen.jraft;

public interface ServerListener {

    void onStarted(Server server);

    void onDestroyed(Server server);
}
