package com.baichen.jraft.transport.exception;

import com.baichen.jraft.exception.JRaftException;

public class TransportClientRequestTimeoutException extends JRaftException {

    public TransportClientRequestTimeoutException() {
    }

    public TransportClientRequestTimeoutException(String message) {
        super(message);
    }

    public TransportClientRequestTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransportClientRequestTimeoutException(Throwable cause) {
        super(cause);
    }

    public TransportClientRequestTimeoutException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
