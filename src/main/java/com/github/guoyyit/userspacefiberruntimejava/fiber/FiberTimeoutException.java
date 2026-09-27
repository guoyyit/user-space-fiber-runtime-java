package com.github.guoyyit.userspacefiberruntimejava.fiber;

/**
 * Thrown when a timed operation (sleep / park with deadline / join with timeout) expires.
 */
public final class FiberTimeoutException extends FiberException {
    public FiberTimeoutException(String message) {
        super(message);
    }
}
