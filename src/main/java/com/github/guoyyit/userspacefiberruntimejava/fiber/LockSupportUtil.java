package com.github.guoyyit.userspacefiberruntimejava.fiber;

import java.util.concurrent.locks.LockSupport;

/** Thin wrapper around {@link LockSupport} for parking non-fiber caller threads. */
final class LockSupportUtil {
    private LockSupportUtil() {
    }

    /** @param permitMillis 0 or negative means block indefinitely until unparked. */
    static void park(long permitMillis) {
        if (permitMillis > 0) {
            LockSupport.parkNanos(permitMillis * 1_000_000L);
        } else {
            LockSupport.park();
        }
    }

    static void unpark(Thread thread) {
        if (thread != null) {
            LockSupport.unpark(thread);
        }
    }
}
