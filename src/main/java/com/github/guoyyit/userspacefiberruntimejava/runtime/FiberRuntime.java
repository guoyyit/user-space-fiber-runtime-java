package com.github.guoyyit.userspacefiberruntimejava.runtime;

import java.util.List;
import java.util.Objects;

/**
 * Public entry point of the user-space cooperative runtime. Spawn fibers,
 * query them, shut the runtime down. All fibers live in this process; the
 * scheduler never relies on OS preemption.
 */
public final class FiberRuntime implements AutoCloseable {

    private final Scheduler scheduler;

    private FiberRuntime(FiberQuotas quotas, Clock clock) {
        this.scheduler = new Scheduler(quotas, clock);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Spawn a root fiber with default options. */
    public <T> FiberHandle<T> spawn(FiberBody<T> body) {
        return spawn(FiberOptions.DEFAULT, body);
    }

    /** Spawn a root fiber with explicit options. */
    public <T> FiberHandle<T> spawn(FiberOptions options, FiberBody<T> body) {
        Objects.requireNonNull(body, "body");
        return scheduler.submit(null, options, body);
    }

    /** Snapshot of all non-terminal fibers (for monitoring / REST). */
    public List<FiberHandle<?>> activeFibers() {
        return scheduler.activeFibers();
    }

    /** Cancel everything and stop the dispatcher. */
    @Override
    public void close() {
        scheduler.shutdown();
    }

    public static final class Builder {

        private FiberQuotas quotas = FiberQuotas.UNLIMITED;
        private Clock clock = new SystemClock();

        private Builder() {
        }

        public Builder quotas(FiberQuotas quotas) {
            this.quotas = Objects.requireNonNull(quotas);
            return this;
        }

        /** Deterministic logical clock (tests). Default: wall clock. */
        public Builder virtualClock(VirtualClock clock) {
            this.clock = Objects.requireNonNull(clock);
            return this;
        }

        public FiberRuntime start() {
            FiberRuntime runtime = new FiberRuntime(quotas, clock);
            runtime.scheduler.start();
            return runtime;
        }
    }
}
