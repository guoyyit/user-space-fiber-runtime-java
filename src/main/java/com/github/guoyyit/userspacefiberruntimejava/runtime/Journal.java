package com.github.guoyyit.userspacefiberruntimejava.runtime;

import java.util.ArrayList;
import java.util.List;

/**
 * Append-only log of completed fiber operations. On replay, already-logged
 * operations return their recorded result instead of re-executing, which
 * restores local state deterministically after a suspension.
 *
 * <p>Entry states: PENDING (started, waiting for an async event) or complete
 * (a value, possibly null, or a recorded failure).
 */
final class Journal {

    private static final Object PENDING = new Object();

    private final List<Object> entries = new ArrayList<>();

    int size() {
        return entries.size();
    }

    boolean isComplete(int index) {
        return index < entries.size() && entries.get(index) != PENDING;
    }

    /** @return logged value, or rethrow the logged failure. */
    @SuppressWarnings("unchecked")
    <V> V replay(int index) {
        Object e = entries.get(index);
        if (e instanceof RecordedFailure f) {
            return sneakyThrow(f.error());
        }
        return (V) e;
    }

    /** Mark the operation as started; its result arrives asynchronously. */
    void beginOp(int index) {
        set(index, PENDING);
    }

    void recordValue(int index, Object value) {
        set(index, value);
    }

    void recordFailure(int index, Throwable error) {
        set(index, new RecordedFailure(error));
    }

    private void set(int index, Object value) {
        while (entries.size() <= index) {
            entries.add(PENDING);
        }
        entries.set(index, value);
    }

    private record RecordedFailure(Throwable error) {
    }

    @SuppressWarnings("unchecked")
    private static <T, E extends Throwable> T sneakyThrow(Throwable t) throws E {
        throw (E) t;
    }
}
