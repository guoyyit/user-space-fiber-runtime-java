package com.github.guoyyit.userspacefiberruntimejava.fiber;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Heap-resident "local variables" of a fiber.
 *
 * <p>The portable runtime saves continuation state on the heap (it cannot move the JVM call
 * stack), so values written here survive {@code park}/{@code resume} cycles even though the
 * fiber occupies no worker thread while suspended. This table is the analogue of the captured
 * local-variable slots of a compiled coroutine state machine.
 *
 * <p>All methods are thread-safe because a fiber may be resumed or cancelled from a different
 * worker/thread than the one it last ran on.
 */
public final class FiberLocals {
    private final Map<String, Object> values = new HashMap<>();

    public synchronized void put(String key, Object value) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        values.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public synchronized <T> T get(String key) {
        return (T) values.get(key);
    }

    @SuppressWarnings("unchecked")
    public synchronized <T> Optional<T> find(String key) {
        return Optional.ofNullable((T) values.get(key));
    }

    public synchronized boolean contains(String key) {
        return values.containsKey(key);
    }

    public synchronized Object remove(String key) {
        return values.remove(key);
    }

    public synchronized void clear() {
        values.clear();
    }

    public synchronized int size() {
        return values.size();
    }
}
