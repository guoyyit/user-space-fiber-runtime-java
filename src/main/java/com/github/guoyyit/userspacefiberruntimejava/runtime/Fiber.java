package com.github.guoyyit.userspacefiberruntimejava.runtime;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Internal fiber record: state machine, journal, parent/child links and
 * cancellation callbacks. A suspended fiber is only this heap object; it
 * holds no execution thread.
 *
 * <p>All mutable scheduling state is guarded by the scheduler monitor.
 * {@link #doneLatch} publishes the terminal outcome to external waiters.
 */
final class Fiber<T> implements FiberHandle<T> {

    final Scheduler scheduler;
    final long id;
    final long seq;
    final String name;
    final FiberPriority priority;
    final Fiber<?> parent;
    final FiberBody<T> body;
    final Journal journal = new Journal();
    final List<Fiber<?>> children = new ArrayList<>();
    final List<Runnable> cancelCallbacks = new ArrayList<>();
    final CountDownLatch doneLatch = new CountDownLatch(1);
    /** Deadline in clock millis; Long.MAX_VALUE when no timeout. */
    final long deadlineMillis;

    // ---- scheduler-guarded mutable state ----
    FiberState state = FiberState.NEW;
    int pendingOpIndex = -1;
    boolean yieldRequested;
    boolean waitingChildren;
    boolean cancelRequested;
    FiberCancellationException cancelCause;
    Outcome<T> pendingOutcome;
    /** Waiters: other fibers blocked in ctx.await(this). */
    List<AwaitRegistration> waiters;
    int waitedTicks;
    int baselineStackDepth = -1;

    volatile Outcome<T> outcome;

    record AwaitRegistration(Fiber<?> waiter, int opIndex) {
    }

    Fiber(Scheduler scheduler, long id, long seq, Fiber<?> parent, FiberOptions options, FiberBody<T> body) {
        this.scheduler = scheduler;
        this.id = id;
        this.seq = seq;
        this.parent = parent;
        this.body = body;
        this.name = options.name() != null ? options.name() : "fiber-" + id;
        this.priority = options.priority();
        this.deadlineMillis = options.timeout() == null
                ? Long.MAX_VALUE
                : scheduler.clock().nowMillis() + options.timeout().toMillis();
    }

    // ---- FiberHandle -------------------------------------------------------

    @Override
    public long id() {
        return id;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public FiberState state() {
        synchronized (scheduler) {
            return state;
        }
    }

    @Override
    public Outcome<T> outcome() {
        return outcome;
    }

    @Override
    public Outcome<T> await() throws InterruptedException {
        doneLatch.await();
        return outcome;
    }

    @Override
    public Outcome<T> await(Duration timeout) throws InterruptedException {
        return doneLatch.await(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
                ? outcome
                : null;
    }

    @Override
    public void cancel() {
        scheduler.requestCancel(this, new FiberCancellationException("cancel requested for " + name));
    }

    @Override
    public String toString() {
        return "Fiber{" + id + "," + name + "," + state() + "}";
    }
}
