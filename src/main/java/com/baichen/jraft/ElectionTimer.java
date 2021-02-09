package com.baichen.jraft;

public interface ElectionTimer extends Lifecycle {

    void reset();

    void subscribeTimeout(Runnable sub);

    void unsubscribeTimeout(Runnable sub);


}
