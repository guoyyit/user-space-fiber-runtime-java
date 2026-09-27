package com.github.guoyyit.userspacefiberruntimejava.fiber;

import java.time.Duration;

/** Immutable configuration for a {@link FiberScheduler}. */
public final class FiberSchedulerConfig {
    final int workerThreads;
    final Clock clock;
    final long agingStepMillis;
    final int agingBonusCap;
    final int maxLiveFibers;
    final int defaultMaxStackDepth;
    final int maxParkedFibers;
    final ChildFailurePolicy defaultChildFailurePolicy;
    final boolean autoStart;

    private FiberSchedulerConfig(Builder b) {
        this.workerThreads = b.workerThreads;
        this.clock = b.clock;
        this.agingStepMillis = b.agingStepMillis;
        this.agingBonusCap = b.agingBonusCap;
        this.maxLiveFibers = b.maxLiveFibers;
        this.defaultMaxStackDepth = b.defaultMaxStackDepth;
        this.maxParkedFibers = b.maxParkedFibers;
        this.defaultChildFailurePolicy = b.defaultChildFailurePolicy;
        this.autoStart = b.autoStart;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int workerThreads() {
        return workerThreads;
    }

    public Clock clock() {
        return clock;
    }

    public long agingStepMillis() {
        return agingStepMillis;
    }

    public int agingBonusCap() {
        return agingBonusCap;
    }

    public int maxLiveFibers() {
        return maxLiveFibers;
    }

    public int defaultMaxStackDepth() {
        return defaultMaxStackDepth;
    }

    public int maxParkedFibers() {
        return maxParkedFibers;
    }

    public ChildFailurePolicy defaultChildFailurePolicy() {
        return defaultChildFailurePolicy;
    }

    public boolean autoStart() {
        return autoStart;
    }

    public static final class Builder {
        private int workerThreads = 1;
        private Clock clock = Clock.system();
        private long agingStepMillis = 10L;
        private int agingBonusCap = Priority.HIGH.weight() - Priority.LOW.weight();
        private int maxLiveFibers = 10_000;
        private int defaultMaxStackDepth = 64;
        private int maxParkedFibers = 10_000;
        private ChildFailurePolicy defaultChildFailurePolicy = ChildFailurePolicy.CANCEL_SIBLINGS;
        private boolean autoStart = true;

        /** Number of worker threads. Use 1 for deterministic replay. */
        public Builder workerThreads(int workerThreads) {
            if (workerThreads < 1) {
                throw new IllegalArgumentException("workerThreads >= 1");
            }
            this.workerThreads = workerThreads;
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder agingStepMillis(long agingStepMillis) {
            this.agingStepMillis = agingStepMillis;
            return this;
        }

        public Builder agingBonusCap(int agingBonusCap) {
            this.agingBonusCap = agingBonusCap;
            return this;
        }

        /** Maximum number of live (non-terminal) fibers; creation beyond it is rejected. */
        public Builder maxLiveFibers(int maxLiveFibers) {
            this.maxLiveFibers = maxLiveFibers;
            return this;
        }

        /** Default per-fiber logical frame-depth quota (single-stack occupancy). */
        public Builder defaultMaxStackDepth(int defaultMaxStackDepth) {
            this.defaultMaxStackDepth = defaultMaxStackDepth;
            return this;
        }

        /** Maximum number of fibers simultaneously suspended (global parked quota). */
        public Builder maxParkedFibers(int maxParkedFibers) {
            this.maxParkedFibers = maxParkedFibers;
            return this;
        }

        public Builder defaultChildFailurePolicy(ChildFailurePolicy policy) {
            this.defaultChildFailurePolicy = policy;
            return this;
        }

        public Builder autoStart(boolean autoStart) {
            this.autoStart = autoStart;
            return this;
        }

        public FiberSchedulerConfig build() {
            if (agingStepMillis <= 0) {
                throw new IllegalArgumentException("agingStepMillis must be positive");
            }
            return new FiberSchedulerConfig(this);
        }
    }
}
