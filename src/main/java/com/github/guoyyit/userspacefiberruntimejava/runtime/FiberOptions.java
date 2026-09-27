package com.github.guoyyit.userspacefiberruntimejava.runtime;

import java.time.Duration;
import java.util.Objects;

/**
 * Spawn-time options: name, base priority and an optional timeout
 * (deadline = spawn time + timeout, evaluated on the runtime clock).
 */
public record FiberOptions(String name, FiberPriority priority, Duration timeout) {

    public static final FiberOptions DEFAULT = new FiberOptions(null, FiberPriority.NORMAL, null);

    public FiberOptions {
        priority = priority == null ? FiberPriority.NORMAL : priority;
        if (timeout != null && (timeout.isNegative() || timeout.isZero())) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }

    public static FiberOptions named(String name) {
        return new FiberOptions(name, FiberPriority.NORMAL, null);
    }

    public FiberOptions withName(String newName) {
        return new FiberOptions(newName, priority, timeout);
    }

    public FiberOptions withPriority(FiberPriority p) {
        return new FiberOptions(name, Objects.requireNonNull(p), timeout);
    }

    public FiberOptions withTimeout(Duration t) {
        return new FiberOptions(name, priority, t);
    }
}
