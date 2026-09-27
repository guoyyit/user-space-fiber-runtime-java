package com.github.guoyyit.userspacefiberruntimejava.fiber;

/**
 * Logical millisecond clock used by the scheduler, enabling deterministic test replay.
 *
 * <p>A clock optionally supports a change listener: the scheduler registers one so that an
 * externally driven {@link ManualClock} can wake idle workers the moment simulated time moves.
 * The wall-clock implementation relies on timed waits instead.
 */
public interface Clock {
    long nowMillis();

    /** Hook used by the runtime; application code normally does not call this. */
    default void setChangeListener(Runnable listener) {
    }

    static Clock system() {
        return SystemClock.INSTANCE;
    }

    static ManualClock manual() {
        return new ManualClock(0L);
    }
}
