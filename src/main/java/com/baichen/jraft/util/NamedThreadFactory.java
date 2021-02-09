package com.baichen.jraft.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

    private String name;

    private boolean daemon;

    private AtomicInteger count = new AtomicInteger(0);

    public NamedThreadFactory(String name, boolean daemon) {
        this.name = name;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {

        Thread thread = new Thread(r);
        thread.setDaemon(daemon);
        thread.setName(name + count.incrementAndGet());
        return thread;
    }
}
