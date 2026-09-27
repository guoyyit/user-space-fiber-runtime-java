package com.github.guoyyit.userspacefiberruntimejava.runtime;

/** Thrown when a runtime resource quota would be exceeded. */
public class QuotaExceededException extends RuntimeException {

    public enum Kind {
        LIVE_FIBERS,
        SUSPENDED_FIBERS,
        STACK_DEPTH
    }

    private final Kind kind;

    public QuotaExceededException(Kind kind, String message) {
        super(message);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }
}
