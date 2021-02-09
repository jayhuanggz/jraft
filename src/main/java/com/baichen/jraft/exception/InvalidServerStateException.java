package com.baichen.jraft.exception;

public class InvalidServerStateException extends JRaftException {

    public InvalidServerStateException() {
    }

    public InvalidServerStateException(String message) {
        super(message);
    }

    public InvalidServerStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidServerStateException(Throwable cause) {
        super(cause);
    }

    public InvalidServerStateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
