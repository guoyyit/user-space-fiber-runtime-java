package com.github.guoyyit.userspacefiberruntimejava.fiber;

/**
 * Controllable clock for deterministic tests: time only advances via {@link #advance(long)} /
 * {@link #set(long)}. Combined with a single worker it makes scheduling order and timeouts
 * exactly reproducible. Each change pings the registered listener (the scheduler) so simulated
 * timers fire without any sleeping thread.
 */
public final class ManualClock implements Clock {
    private long now;
    private volatile Runnable listener;

    public ManualClock(long startMillis) {
        this.now = startMillis;
    }

    @Override
    public long nowMillis() {
        return now;
    }

    @Override
    public void setChangeListener(Runnable listener) {
        this.listener = listener;
    }

    public void set(long millis) {
        synchronized (this) {
            if (millis < now) {
                throw new IllegalArgumentException("manual clock cannot move backwards");
            }
            this.now = millis;
        }
        Runnable l = listener;
        if (l != null) {
            l.run();
        }
    }

    public void advance(long deltaMillis) {
        if (deltaMillis < 0) {
            throw new IllegalArgumentException("delta must be non-negative");
        }
        synchronized (this) {
            this.now += deltaMillis;
        }
        Runnable l = listener;
        if (l != null) {
            l.run();
        }
    }
}
