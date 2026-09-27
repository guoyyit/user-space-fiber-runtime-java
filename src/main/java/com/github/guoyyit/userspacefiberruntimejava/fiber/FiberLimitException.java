package com.github.guoyyit.userspacefiberruntimejava.fiber;

/** Thrown when creating a fiber would exceed the scheduler-wide live-fiber quota. */
public final class FiberLimitException extends QuotaExceededException {
    public FiberLimitException(String message) {
        super(message);
    }
}
