package com.baichen.jraft.impl;

import com.baichen.jraft.ElectionTimer;
import com.baichen.jraft.util.RepeatableHashedWheelTimer;
import com.baichen.jraft.util.RepeatableTimer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class RandomElectionTimer implements ElectionTimer {

    private static final Logger LOGGER = LogManager.getLogger(RandomElectionTimer.class);

    private int min;

    private int max;

    private final Random random = new Random();

    private volatile Runnable timerSub;

    private RepeatableTimer.TimerTask timerTask;

    private RepeatableTimer timer;

    public RandomElectionTimer(int min, int max) {
        this.min = min;
        this.max = max;
        this.timer = new RepeatableHashedWheelTimer(20, 50, TimeUnit.MILLISECONDS);
    }

    @Override
    public void reset() {
        timerTask.reset();
    }

    @Override
    public void subscribeTimeout(Runnable sub) {
        this.timerSub = sub;
    }

    @Override
    public void unsubscribeTimeout(Runnable sub) {
        this.timerSub = null;
    }

    private long getInterval() {
        return random.nextInt(max + 1 - min) + min;
    }

    @Override
    public void start() {

        timerTask = timer.schedule(() -> {
            if (timerSub != null) {
                try {
                    timerSub.run();
                } catch (Throwable e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }
        }, this::getInterval);

    }

    @Override
    public void destroy() {
        timerTask.cancel();
        timer.shutdown();

    }


}
