package com.baichen.jraft.exception;

public class FailedToAcquireLockException extends JRaftException {

    public FailedToAcquireLockException() {
    }

    public FailedToAcquireLockException(String message) {
        super(message);
    }

    public FailedToAcquireLockException(String message, Throwable cause) {
        super(message, cause);
    }

    public FailedToAcquireLockException(Throwable cause) {
        super(cause);
    }

    public FailedToAcquireLockException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
