package com.github.guoyyit.userspacefiberruntimejava.fiber;

/** Thrown when a single fiber's frame depth exceeds its per-stack quota. */
public final class StackQuotaException extends QuotaExceededException {
    public StackQuotaException(String message) {
        super(message);
    }
}
