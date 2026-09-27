package com.github.guoyyit.userspacefiberruntimejava.runtime;

import java.time.Duration;

/**
 * External reference to a fiber: query state, wait for termination, cancel.
 */
public interface FiberHandle<T> {

    long id();

    String name();

    FiberState state();

    /** @return terminal outcome, or null while still running. */
    Outcome<T> outcome();

    /** Block the caller until the fiber terminates. */
    Outcome<T> await() throws InterruptedException;

    /** Block the caller up to the timeout; null if still running. */
    Outcome<T> await(Duration timeout) throws InterruptedException;

    /** Convenience: await + rethrow failure/cancellation. */
    default T result() throws InterruptedException {
        return await().getOrThrow();
    }

    /** Request cooperative cancellation (cascades to descendants). */
    void cancel();

    default boolean isDone() {
        return state().isTerminal();
    }
}
