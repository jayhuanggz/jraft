package com.baichen.jraft.util;

import java.util.function.Supplier;

public interface RepeatableTimer {

    TimerTask schedule(Runnable taskUnit, Supplier<Long> delaySupplier);

    void shutdown();

    interface TimerTask {

        void cancel();

        void reset();


    }
}
