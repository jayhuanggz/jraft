package com.baichen.jraft.transport.exception;

import com.baichen.jraft.exception.JRaftException;

public class UnexpectedEndOfExecutionException extends JRaftException {

    public UnexpectedEndOfExecutionException() {
    }

    public UnexpectedEndOfExecutionException(String message) {
        super(message);
    }

    public UnexpectedEndOfExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnexpectedEndOfExecutionException(Throwable cause) {
        super(cause);
    }

    public UnexpectedEndOfExecutionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
