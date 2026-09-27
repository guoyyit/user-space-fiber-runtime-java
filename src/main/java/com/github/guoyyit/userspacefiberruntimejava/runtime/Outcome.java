package com.github.guoyyit.userspacefiberruntimejava.runtime;

import java.util.concurrent.CancellationException;

/**
 * Terminal result of a fiber. Exactly one of success / failure / cancellation.
 */
public sealed interface Outcome<T> {

    record Success<T>(T value) implements Outcome<T> {
    }

    record Failed<T>(Throwable error) implements Outcome<T> {
    }

    record Cancelled<T>(CancellationException reason) implements Outcome<T> {
    }

    /** @return value on success, rethrows failure / cancellation otherwise. */
    default T getOrThrow() {
        if (this instanceof Success<T> s) {
            return s.value();
        }
        if (this instanceof Failed<T> f) {
            throw new FiberFailedException("fiber failed", f.error());
        }
        throw ((Cancelled<T>) this).reason();
    }
}
