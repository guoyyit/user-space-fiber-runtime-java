package com.github.guoyyit.userspacefiberruntimejava.runtime;

/**
 * Lifecycle states of a fiber.
 *
 * <pre>
 * NEW -&gt; RUNNABLE -&gt; RUNNING -&gt; SUSPENDED -&gt; RUNNABLE ... -&gt; COMPLETED | FAILED | CANCELLED
 * </pre>
 * Terminal states: COMPLETED, FAILED, CANCELLED.
 */
public enum FiberState {
    /** Created but not yet made visible to the scheduler. */
    NEW,
    /** In the scheduler ready queue, waiting for a dispatch turn. */
    RUNNABLE,
    /** Currently executing its body on the dispatcher thread. */
    RUNNING,
    /** Parked (timer / await / yield). Holds no execution thread. */
    SUSPENDED,
    /** Body returned normally. */
    COMPLETED,
    /** Body threw an uncaught exception. */
    FAILED,
    /** Cancellation was requested and delivered. */
    CANCELLED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }
}
