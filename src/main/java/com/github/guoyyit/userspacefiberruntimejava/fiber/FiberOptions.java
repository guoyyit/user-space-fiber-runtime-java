package com.github.guoyyit.userspacefiberruntimejava.fiber;

import java.time.Duration;

/** Immutable creation options for a fiber. */
public final class FiberOptions {
    private final String name;
    private final Priority priority;
    private final Duration timeout;
    private final ChildFailurePolicy childFailurePolicy;
    private final Integer maxStackDepth;

    public FiberOptions(String name, Priority priority, Duration timeout,
                        ChildFailurePolicy childFailurePolicy, Integer maxStackDepth) {
        this.name = name;
        this.priority = priority;
        this.timeout = timeout;
        this.childFailurePolicy = childFailurePolicy;
        this.maxStackDepth = maxStackDepth;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String name() {
        return name;
    }

    public Priority priority() {
        return priority;
    }

    public Duration timeout() {
        return timeout;
    }

    public ChildFailurePolicy childFailurePolicy() {
        return childFailurePolicy;
    }

    public Integer maxStackDepth() {
        return maxStackDepth;
    }

    public static final class Builder {
        private String name;
        private Priority priority = Priority.NORMAL;
        private Duration timeout;
        private ChildFailurePolicy childFailurePolicy = ChildFailurePolicy.CANCEL_SIBLINGS;
        private Integer maxStackDepth;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder priority(Priority priority) {
            this.priority = priority;
            return this;
        }

        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder childFailurePolicy(ChildFailurePolicy childFailurePolicy) {
            this.childFailurePolicy = childFailurePolicy;
            return this;
        }

        public Builder maxStackDepth(Integer maxStackDepth) {
            this.maxStackDepth = maxStackDepth;
            return this;
        }

        public FiberOptions build() {
            return new FiberOptions(name, priority, timeout, childFailurePolicy, maxStackDepth);
        }
    }
}
