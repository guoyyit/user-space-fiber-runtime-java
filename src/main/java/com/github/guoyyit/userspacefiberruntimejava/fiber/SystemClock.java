package com.github.guoyyit.userspacefiberruntimejava.fiber;

/** Wall-clock time source. */
final class SystemClock implements Clock {
    static final SystemClock INSTANCE = new SystemClock();

    private SystemClock() {
    }

    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
