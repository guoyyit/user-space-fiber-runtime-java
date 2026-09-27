package com.github.guoyyit.userspacefiberruntimejava.runtime;

/** Wall-clock time. */
final class SystemClock implements Clock {
    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
