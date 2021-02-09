package com.baichen.jraft.util;

import com.google.common.base.Preconditions;
import org.jctools.queues.MpscLinkedQueue;

import javax.annotation.concurrent.ThreadSafe;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Queue;
import java.util.Timer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

@ThreadSafe
public class RepeatableHashedWheelTimer implements RepeatableTimer {

    private static final Logger LOGGER = LogManager.getLogManager().getLogger(RepeatableHashedWheelTimer.class.getName());

    // Task created but not added to bucket
    private static final int TASK_STATE_CREATED = 1;

    // task added to bucket, waiting to expire
    private static final int TASK_STATE_WAITING = 2;

    // task expired
    private static final int TASK_STATE_EXPIRED = 3;

    // task timer has been reset, waiting to be added to bucket again
    private static final int TASK_STATE_RESET = 4;

    // task has been cancelled
    private static final int TASK_STATE_CANCELLED = 5;

    private Bucket[] buckets;

    private TimeUnit timeUnit;

    private long tickDuration;

    private TimerWorker worker;

    private boolean shutdown = false;

    private long wheelDuration;

    private final Queue<TimerTaskImpl> cancelledTasks = new MpscLinkedQueue<>();

    /**
     * Tasks that has been added by calling schedule() but has not put in a bucket
     */
    private Queue<TimerTaskImpl> tasks = new MpscLinkedQueue<>();

    public RepeatableHashedWheelTimer(int wheelSize, long tickDuration, TimeUnit timeUnit) {
        this(wheelSize, tickDuration, timeUnit, 16, Integer.MAX_VALUE);
    }

    public RepeatableHashedWheelTimer(int wheelSize, long tickDuration, TimeUnit timeUnit, int bucketInitialSize, int bucketCapacity) {
        Preconditions.checkArgument(wheelSize > 0);
        Preconditions.checkArgument(tickDuration > 0);
        Preconditions.checkArgument(timeUnit != null);
        Preconditions.checkArgument(bucketInitialSize > 0);
        Preconditions.checkArgument(bucketCapacity > 0);

        this.timeUnit = timeUnit;
        this.tickDuration = tickDuration;
        this.wheelDuration = this.tickDuration * wheelSize;
        this.buckets = new Bucket[wheelSize];
        for (int i = 0; i < wheelSize; i++) {
            this.buckets[i] = new Bucket(bucketInitialSize, bucketCapacity, timeUnit);
        }
        worker = new TimerWorker(this);
        worker.setDaemon(true);
        worker.setName("RepeatableHashedWheelTimer-Worker");
        worker.start();
    }

    @Override
    public TimerTask schedule(Runnable taskUnit, Supplier<Long> delaySupplier) {

        Preconditions.checkArgument(!shutdown, "Timer has shutdown!");
        TimerTaskImpl task = new TimerTaskImpl(this, taskUnit, delaySupplier);
        Queue<TimerTaskImpl> savedTasks = tasks;
        if (savedTasks != null) {
            savedTasks.offer(task);
        }
        return task;


    }


    private Bucket findBucket(long expireTime) {


        long durations = expireTime / tickDuration;

        if (expireTime % tickDuration > 0) {
            durations++;
        }
        int tick = (int) (durations % buckets.length);
        return buckets[tick];

    }


    @Override
    public void shutdown() {
        synchronized (this) {

            if (shutdown) {
                return;
            }

            shutdown = true;
            tasks.clear();
            cancelledTasks.clear();
            worker.terminate();
            try {
                worker.join();
            } catch (InterruptedException e) {
            }
            for (Bucket bucket : buckets) {
                bucket.clear();
            }
            buckets = null;
        }
    }

    private String getStackTrace(Throwable e) {

        StringWriter result = new StringWriter(256);
        e.printStackTrace(new PrintWriter(result));
        return result.toString();

    }

    private static final class TimerWorker extends Thread {

        private static final AtomicIntegerFieldUpdater<TimerWorker> UPDATER = AtomicIntegerFieldUpdater.newUpdater(TimerWorker.class, "stopped");

        private volatile int stopped;

        private RepeatableHashedWheelTimer timer;


        public TimerWorker(RepeatableHashedWheelTimer timer) {
            this.timer = timer;
        }

        @Override
        public void run() {

            RepeatableHashedWheelTimer timerCopy = this.timer;

            if (timerCopy == null) {
                return;
            }
            Bucket[] buckets = timerCopy.buckets;
            if (buckets == null) {
                return;
            }
            int wheelSize = buckets.length;

            while (stopped == 0) {

                timerCopy = this.timer;

                // find current tick
                long now = timerCopy.timeUnit.convert(System.nanoTime(), TimeUnit.NANOSECONDS);

                long durations = now / timerCopy.tickDuration;

                if (now % timerCopy.tickDuration > 0) {
                    durations++;
                }
                int currentTick = (int) (durations % wheelSize);
                long currentTickExpiration = timerCopy.tickDuration * (durations + 1);

                // get the bucket, run expired tasks in the bucket
                Bucket bucket = timerCopy.buckets[currentTick];
                bucket.runExpiredTasks();
                handleCancelledTasks();
                putTasksInBuckets();
                waitForNextTick(currentTickExpiration);


            }
        }

        private void putTasksInBuckets() {
            RepeatableHashedWheelTimer timerCopy = this.timer;
            if (timerCopy != null) {
                TimerTaskImpl task;
                while ((task = timerCopy.tasks.poll()) != null) {
                    task.transferBucket();
                }
            }


        }


        private void handleCancelledTasks() {


            RepeatableHashedWheelTimer timerCopy = this.timer;
            if (timerCopy != null) {
                Queue<TimerTaskImpl> cancelledTasks = timerCopy.cancelledTasks;
                TimerTaskImpl task;
                while ((task = cancelledTasks.poll()) != null) {
                    task.remove();
                }

            }


        }


        void waitForNextTick(long expiration) {

            while (stopped == 0) {
                long now = timer.timeUnit.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
                long diffInMs = timer.timeUnit.toMillis(expiration - now);
                if (diffInMs > 0) {
                    try {
                        Thread.sleep(diffInMs);
                        break;
                    } catch (InterruptedException e) {

                    }
                } else {
                    break;
                }
            }


        }

        void terminate() {
            if (UPDATER.compareAndSet(this, 0, 1)) {
                // interrupt as it might be waiting for next tick
                this.interrupt();
                timer = null;
            }
        }

    }

    private static final class TimerTaskImpl implements TimerTask, Comparable<TimerTaskImpl> {

        private static final AtomicIntegerFieldUpdater<TimerTaskImpl> TASK_STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(TimerTaskImpl.class, "state");

        private RepeatableHashedWheelTimer timer;

        private Runnable runnable;

        private long delay;

        private Supplier<Long> delaySupplier;

        private long nextExecutionTime;

        private Bucket bucket;

        private volatile int state = TASK_STATE_CREATED;

        public TimerTaskImpl(RepeatableHashedWheelTimer timer, Runnable runnable, long delay, Supplier<Long> delaySupplier) {
            this.runnable = runnable;
            this.delay = delay;
            this.delaySupplier = delaySupplier;
            this.timer = timer;
        }

        public TimerTaskImpl(RepeatableHashedWheelTimer timer, Runnable runnable, Supplier<Long> delaySupplier) {
            this(timer, runnable, delaySupplier.get(), delaySupplier);
        }

        @Override
        public void cancel() {
            int curState;
            while ((curState = state) != TASK_STATE_CANCELLED) {
                if (TASK_STATE_UPDATER.compareAndSet(this, curState, TASK_STATE_CANCELLED)) {
                    timer.cancelledTasks.offer(this);
                    break;
                }
            }
        }

        @Override
        public void reset() {
            int curState;
            while ((curState = state) != TASK_STATE_CANCELLED && curState != TASK_STATE_RESET
                    && curState != TASK_STATE_CREATED) {
                if (TASK_STATE_UPDATER.compareAndSet(this, curState, TASK_STATE_RESET)) {
                    timer.tasks.offer(this);
                    break;
                }
            }
        }

        public void transferBucket() {

            int curState;
            while ((curState = state) != TASK_STATE_CANCELLED) {
                if (TASK_STATE_UPDATER.compareAndSet(this, curState, TASK_STATE_WAITING)) {
                    if (curState == TASK_STATE_RESET) {
                        remove();
                    }
                    updateNextExecutionTime(curState == TASK_STATE_RESET);
                    Bucket newBucket = timer.findBucket(nextExecutionTime);
                    newBucket.addTask(this);
                    break;
                }
            }
        }

        void remove() {
            if (bucket != null) {
                bucket.remove(this);
                bucket = null;
            }

        }

        void updateNextExecutionTime(boolean updateDelay) {

            if (updateDelay && delaySupplier != null) {
                long nextDelay = delaySupplier.get();
                if (nextDelay <= 0) {
                    throw new IllegalArgumentException("Illegal next delay: " + nextDelay);
                }
                this.delay = nextDelay;
            }
            long now = timer.timeUnit.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
            nextExecutionTime = now + delay;

        }

        int calculateRemainingRound() {
            long now = timer.timeUnit.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
            long diff = nextExecutionTime - now;
            if (diff <= 0) {
                return 0;
            }

            int remainingRound = (int) (diff / timer.wheelDuration);
            if (diff % timer.wheelDuration > 0) {
                remainingRound++;
            }
            return remainingRound;
        }

        @Override
        public int compareTo(TimerTaskImpl o) {
            return calculateRemainingRound() - o.calculateRemainingRound();
        }

        void expire() {
            if (TASK_STATE_UPDATER.compareAndSet(this, TASK_STATE_WAITING, TASK_STATE_EXPIRED)) {
                try {
                    if (runnable != null) {
                        runnable.run();
                    }
                } catch (Throwable e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Got exception while executing task: " + timer.getStackTrace(e));
                    }
                }
            }
        }
    }


    private static class Bucket {

        /**
         * Use a min binary heap to store tasks, ordered by remaining round
         */
        private BinaryHeap<TimerTaskImpl> tasks;

        private TimeUnit timeUnit;


        Bucket(int initialSize, int capacity, TimeUnit timeUnit) {
            tasks = new BinaryHeap<>(initialSize, capacity);
            this.timeUnit = timeUnit;
        }

        void clear() {
            tasks.clear();

        }

        void remove(TimerTaskImpl task) {
            tasks.remove(task);
        }


        void addTask(TimerTaskImpl task) {
            this.tasks.add(task);
            task.bucket = this;

        }

        public void runExpiredTasks() {
            TimerTaskImpl task;
            while (true) {
                task = tasks.peek();
                if (task == null) {
                    break;
                }

                if (task.calculateRemainingRound() > 0) {
                    // if the top task is not due to expire, no need to check the rest as
                    // tasks are already ordered by remaining round
                    break;
                } else {
                    tasks.pop();
                }


                if (task.state != TASK_STATE_CANCELLED && task.state != TASK_STATE_RESET) {
                    long now = timeUnit.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
                    long diffInMs = timeUnit.toMillis(task.nextExecutionTime - now);
                    // wait for the task to actually expire. Because tasks in the same bucket with
                    // the same remaining round may
                    // have different expiration
                    if (diffInMs > 0) {
                        try {
                            Thread.sleep(diffInMs);
                        } catch (InterruptedException e) {
                        }
                    }
                    task.expire();


                }

            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        RepeatableHashedWheelTimer timer = new RepeatableHashedWheelTimer(20, 50, TimeUnit.MILLISECONDS);

        long start = System.currentTimeMillis();
        TimerTask task = timer.schedule(() -> {

            System.out.println("running after: " + (System.currentTimeMillis() - start));
        }, () -> 10000l);

        new Timer().scheduleAtFixedRate(new java.util.TimerTask() {
            @Override
            public void run() {
                task.reset();
            }
        }, 1000, 1000);

        Thread.sleep(Long.MAX_VALUE);

        timer.shutdown();
    }
}
