package com.baichen.jraft;

public interface HeartbeatTimer extends Lifecycle {

    void subscribe(Runnable sub);

    void unsubscribe();


}
