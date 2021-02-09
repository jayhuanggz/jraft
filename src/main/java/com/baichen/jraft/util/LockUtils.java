package com.baichen.jraft.util;

import com.baichen.jraft.exception.FailedToAcquireLockException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;


public class LockUtils {

    private static final Logger LOGGER = LogManager.getLogger(LockUtils.class.getName());

    public static boolean tryLock(String name, Lock lock, long waitTime, TimeUnit timeUnit, int currentAttempts, int maxAttempts) {
        boolean locked = false;
        try {
            locked = lock.tryLock(waitTime, timeUnit);
        } catch (InterruptedException e) {
        }

        if (locked) {
            return locked;
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(String.format("Failed to acquire lock %s for %s %s, currentAttempts: %s, maxAttempts: %s",
                    name, waitTime, timeUnit.name(), currentAttempts, maxAttempts));
        }

        if (currentAttempts >= maxAttempts) {
            return false;
        }

        return tryLock(name, lock, waitTime, timeUnit, ++currentAttempts, maxAttempts);

    }

    public static void lock(String name, Lock lock, long waitTime, TimeUnit timeUnit, int currentAttempts, int maxAttempts) {

        if (!tryLock(name, lock, waitTime, timeUnit, currentAttempts, maxAttempts)) {
            throw new FailedToAcquireLockException(String.format("Failed to acquire lock %s for %s %s after %s attempts!",
                    name, waitTime, timeUnit.name(), maxAttempts));
        }

    }
}
