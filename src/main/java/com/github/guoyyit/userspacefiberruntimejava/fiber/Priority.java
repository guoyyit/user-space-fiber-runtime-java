package com.github.guoyyit.userspacefiberruntimejava.fiber;

/**
 * Scheduling priority of a fiber.
 * Higher weight is scheduled first (subject to priority aging).
 */
public enum Priority {
    LOW(0),
    NORMAL(5),
    HIGH(10);

    private final int weight;

    Priority(int weight) {
        this.weight = weight;
    }

    public int weight() {
        return weight;
    }
}
