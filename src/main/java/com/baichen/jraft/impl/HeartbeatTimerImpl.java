package com.baichen.jraft.impl;

import com.baichen.jraft.HeartbeatTimer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class HeartbeatTimerImpl extends TimerTask implements HeartbeatTimer {

    private static final Logger LOGGER = LogManager.getLogger(HeartbeatTimerImpl.class);

    private Timer timer;

    private int interval;

    private volatile Runnable sub;

    private AtomicBoolean busy = new AtomicBoolean(false);

    public HeartbeatTimerImpl(int interval) {
        this.interval = interval;
    }


    @Override
    public void start() {

        timer = new Timer();
        timer.scheduleAtFixedRate(this, interval, interval);
    }

    @Override
    public void destroy() {

        sub = null;

        if (timer != null) {
            timer.cancel();
        }
    }

    @Override
    public void run() {

        if (busy.compareAndSet(false, true)) {

            try {
                if (sub != null) {
                    try {
                        sub.run();

                    } catch (Throwable e) {
                        LOGGER.warn(e.getMessage(), e);
                    }
                }

            } finally {
                busy.set(false);
            }
        }


    }


    @Override
    public void subscribe(Runnable sub) {
        this.sub = sub;
    }

    @Override
    public void unsubscribe() {
        this.sub = null;
    }
}
