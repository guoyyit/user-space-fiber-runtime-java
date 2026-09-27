package com.github.guoyyit.userspacefiberruntimejava.fiber;

/** Thrown when parking (suspending) a fiber would exceed the global parked-fiber quota. */
public final class ParkLimitException extends QuotaExceededException {
    public ParkLimitException(String message) {
        super(message);
    }
}
