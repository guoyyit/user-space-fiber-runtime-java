package com.github.guoyyit.userspacefiberruntimejava.runtime;

/**
 * Time source for the scheduler. Real time ({@link SystemClock}) or a
 * deterministic logical clock for tests ({@link VirtualClock}).
 */
interface Clock {
    long nowMillis();
}
