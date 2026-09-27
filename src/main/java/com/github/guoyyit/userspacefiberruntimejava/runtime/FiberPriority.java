package com.github.guoyyit.userspacefiberruntimejava.runtime;

/**
 * Base scheduling priority. The scheduler combines the base priority with an
 * aging term so that low priority fibers cannot starve (see docs).
 */
public enum FiberPriority {
    LOW(0),
    NORMAL(5),
    HIGH(9);

    private final int level;

    FiberPriority(int level) {
        this.level = level;
    }

    public int level() {
        return level;
    }
}
