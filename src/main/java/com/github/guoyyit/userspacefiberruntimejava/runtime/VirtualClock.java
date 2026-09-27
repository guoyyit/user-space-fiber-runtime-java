package com.github.guoyyit.userspacefiberruntimejava.runtime;

/**
 * Deterministic logical clock for tests: the scheduler jumps time straight
 * to the next timer deadline when idle, so replays are fast and reproducible.
 */
public final class VirtualClock implements Clock {

    private long now;

    public VirtualClock() {
        this(0);
    }

    public VirtualClock(long startMillis) {
        this.now = startMillis;
    }

    @Override
    public synchronized long nowMillis() {
        return now;
    }

    /** Advance logical time (never backwards). Called by the scheduler only. */
    public synchronized void advanceTo(long deadlineMillis) {
        now = Math.max(now, deadlineMillis);
    }
}
