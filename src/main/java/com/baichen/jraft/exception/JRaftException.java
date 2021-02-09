package com.baichen.jraft.exception;

public class JRaftException extends RuntimeException {

    public JRaftException() {
    }

    public JRaftException(String message) {
        super(message);
    }

    public JRaftException(String message, Throwable cause) {
        super(message, cause);
    }

    public JRaftException(Throwable cause) {
        super(cause);
    }

    public JRaftException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
