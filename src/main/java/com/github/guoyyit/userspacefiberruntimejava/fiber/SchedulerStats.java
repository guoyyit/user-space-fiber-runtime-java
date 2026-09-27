package com.github.guoyyit.userspacefiberruntimejava.fiber;

/** Point-in-time counters of a {@link FiberScheduler}. */
public final class SchedulerStats {
    public final int liveFibers;
    public final int readyFibers;
    public final int parkedFibers;
    public final long completed;
    public final long failed;
    public final long cancelled;

    public SchedulerStats(int liveFibers, int readyFibers, int parkedFibers,
                          long completed, long failed, long cancelled) {
        this.liveFibers = liveFibers;
        this.readyFibers = readyFibers;
        this.parkedFibers = parkedFibers;
        this.completed = completed;
        this.failed = failed;
        this.cancelled = cancelled;
    }

    @Override
    public String toString() {
        return "SchedulerStats{live=" + liveFibers + ", ready=" + readyFibers
                + ", parked=" + parkedFibers + ", completed=" + completed
                + ", failed=" + failed + ", cancelled=" + cancelled + '}';
    }
}
