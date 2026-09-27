package com.github.guoyyit.userspacefiberruntimejava.runtime;

/** Cancellation caused by a timeout / deadline expiring. */
public class TimeoutCancellationException extends FiberCancellationException {
    public TimeoutCancellationException(String message) {
        super(message);
    }
}
