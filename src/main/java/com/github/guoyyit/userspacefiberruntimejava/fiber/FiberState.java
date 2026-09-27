package com.github.guoyyit.userspacefiberruntimejava.fiber;

/**
 * Lifecycle state of a fiber.
 */
public enum FiberState {
    NEW,
    READY,
    RUNNING,
    /** Suspended by an explicit park with no deadline. */
    SUSPENDED,
    /** Suspended until a deadline fires (sleep / timeout). */
    TIMED_WAITING,
    /** Suspended until one or more other fibers terminate (join). */
    WAITING,
    /** Terminated normally. */
    COMPLETED,
    /** Terminated because the body threw an exception. */
    FAILED,
    /** Terminated because it was cancelled (manual / timeout / parent / shutdown). */
    CANCELLED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }

    public boolean isSuspended() {
        return this == SUSPENDED || this == TIMED_WAITING || this == WAITING;
    }
}
