package com.github.guoyyit.userspacefiberruntimejava.runtime;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Cooperative dispatcher: a single thread runs fiber bodies up to their next
 * suspension point. Ready fibers are picked by (base priority + aging) with
 * submission-sequence tie-breaking, so the order is fair and reproducible.
 *
 * <p>All scheduling state is guarded by {@code synchronized (this)}. The
 * dispatcher parks with {@code wait()} on this monitor when idle, so
 * submissions and cancellations from other threads cannot be missed.
 */
final class Scheduler {

    private static final int PRIORITY_WEIGHT = 10;

    private final FiberQuotas quotas;
    private final Clock clock;
    private final List<Fiber<?>> ready = new ArrayList<>();
    private final PriorityQueue<TimerEntry> timers =
            new PriorityQueue<>((a, b) -> a.deadlineMillis != b.deadlineMillis
                    ? Long.compare(a.deadlineMillis, b.deadlineMillis)
                    : Long.compare(a.seq, b.seq));
    private final Set<Fiber<?>> registry = new LinkedHashSet<>();

    private long nextSeq;
    private int suspendedCount;
    private volatile boolean running;
    private Thread dispatcher;

    private record TimerEntry(long deadlineMillis, long seq, Fiber<?> fiber, int opIndex, boolean deadlineCancel) {
    }

    Scheduler(FiberQuotas quotas, Clock clock) {
        this.quotas = quotas;
        this.clock = clock;
    }

    // ---- submission / lifecycle -------------------------------------------

    <T> Fiber<T> submit(Fiber<?> parent, FiberOptions options, FiberBody<T> body) {
        synchronized (this) {
            if (!running && dispatcher != null) {
                throw new IllegalStateException("runtime is shut down");
            }
            if (registry.size() >= quotas.maxLiveFibers()) {
                throw new QuotaExceededException(QuotaExceededException.Kind.LIVE_FIBERS,
                        "live fiber limit " + quotas.maxLiveFibers() + " reached");
            }
            Fiber<T> fiber = new Fiber<>(this, nextSeq, nextSeq++, parent, options, body);
            registry.add(fiber);
            if (parent != null) {
                parent.children.add(fiber);
            }
            fiber.state = FiberState.RUNNABLE;
            ready.add(fiber);
            if (fiber.deadlineMillis != Long.MAX_VALUE) {
                timers.add(new TimerEntry(fiber.deadlineMillis, fiber.seq, fiber, -1, true));
            }
            notifyAll();
            return fiber;
        }
    }

    void start() {
        synchronized (this) {
            if (running) {
                return;
            }
            running = true;
            dispatcher = new Thread(this::runLoop, "fiber-dispatcher");
            dispatcher.setDaemon(true);
            dispatcher.start();
        }
    }

    void shutdown() {
        Thread t;
        synchronized (this) {
            running = false;
            for (Fiber<?> f : new ArrayList<>(registry)) {
                if (f.parent == null) {
                    cancelLocked(f, new FiberCancellationException("runtime shutdown"));
                }
            }
            notifyAll();
            t = dispatcher;
        }
        if (t != null) {
            try {
                t.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    List<FiberHandle<?>> activeFibers() {
        synchronized (this) {
            return List.copyOf(registry);
        }
    }

    // ---- cancellation ------------------------------------------------------

    void requestCancel(Fiber<?> fiber, FiberCancellationException cause) {
        synchronized (this) {
            cancelLocked(fiber, cause);
            notifyAll();
        }
    }

    /** Cascade + delivery. Must hold the monitor. */
    private void cancelLocked(Fiber<?> fiber, FiberCancellationException cause) {
        if (fiber.state.isTerminal()) {
            return;
        }
        if (!fiber.cancelRequested) {
            fiber.cancelRequested = true;
            fiber.cancelCause = cause;
        }
        for (Fiber<?> child : new ArrayList<>(fiber.children)) {
            cancelLocked(child, cause);
        }
        if (fiber.state == FiberState.SUSPENDED && fiber.pendingOpIndex >= 0) {
            // Interrupt the parked fiber: its pending op completes with the
            // cancellation, delivered on the next replay.
            fiber.journal.recordFailure(fiber.pendingOpIndex, fiber.cancelCause);
            fiber.pendingOpIndex = -1;
            wakeLocked(fiber);
        }
    }

    // ---- suspension services (dispatcher thread) ---------------------------

    void registerTimer(Fiber<?> fiber, int opIndex, long deadlineMillis) {
        synchronized (this) {
            timers.add(new TimerEntry(deadlineMillis, fiber.seq, fiber, opIndex, false));
        }
    }

    void registerAwait(Fiber<?> target, Fiber<?> waiter, int opIndex) {
        synchronized (this) {
            if (target.waiters == null) {
                target.waiters = new ArrayList<>();
            }
            target.waiters.add(new Fiber.AwaitRegistration(waiter, opIndex));
        }
    }

    void checkSuspendQuota() {
        synchronized (this) {
            if (suspendedCount >= quotas.maxSuspendedFibers()) {
                throw new QuotaExceededException(QuotaExceededException.Kind.SUSPENDED_FIBERS,
                        "suspended fiber limit " + quotas.maxSuspendedFibers() + " reached");
            }
        }
    }

    void checkStackDepth(Fiber<?> fiber) {
        int depth = Thread.currentThread().getStackTrace().length - fiber.baselineStackDepth;
        if (depth > quotas.maxStackDepth()) {
            throw new QuotaExceededException(QuotaExceededException.Kind.STACK_DEPTH,
                    "stack depth " + depth + " exceeds limit " + quotas.maxStackDepth());
        }
    }

    FiberQuotas quotas() {
        return quotas;
    }

    Clock clock() {
        return clock;
    }

    // ---- dispatcher loop ----------------------------------------------------

    private void runLoop() {
        while (true) {
            Fiber<?> next;
            synchronized (this) {
                while (true) {
                    fireTimersLocked();
                    next = pickReadyLocked();
                    if (next != null) {
                        break;
                    }
                    if (!running && registry.isEmpty()) {
                        return;
                    }
                    long deadline = timers.isEmpty() ? Long.MAX_VALUE : timers.peek().deadlineMillis;
                    if (clock instanceof VirtualClock vc && deadline != Long.MAX_VALUE) {
                        // Deterministic mode: jump logical time, never sleep.
                        vc.advanceTo(deadline);
                        continue;
                    }
                    try {
                        if (deadline == Long.MAX_VALUE) {
                            wait();
                        } else {
                            long remaining = deadline - clock.nowMillis();
                            if (remaining <= 0) {
                                continue;
                            }
                            wait(remaining);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                next.state = FiberState.RUNNING;
                next.waitedTicks = 0;
                next.baselineStackDepth = Thread.currentThread().getStackTrace().length;
            }
            runOne(next);
        }
    }

    private Fiber<?> pickReadyLocked() {
        Fiber<?> best = null;
        int bestScore = Integer.MIN_VALUE;
        for (Fiber<?> f : ready) {
            int score = f.priority.level() * PRIORITY_WEIGHT + f.waitedTicks;
            if (score > bestScore || (score == bestScore && best != null && f.seq < best.seq)) {
                best = f;
                bestScore = score;
            }
        }
        if (best != null) {
            ready.remove(best);
            for (Fiber<?> f : ready) {
                f.waitedTicks++;
            }
        }
        return best;
    }

    private void fireTimersLocked() {
        long now = clock.nowMillis();
        while (!timers.isEmpty() && timers.peek().deadlineMillis <= now) {
            TimerEntry t = timers.poll();
            Fiber<?> f = t.fiber();
            if (t.deadlineCancel()) {
                if (!f.state.isTerminal()) {
                    cancelLocked(f, new TimeoutCancellationException(
                            "deadline expired for " + f.name));
                }
            } else if (f.state == FiberState.SUSPENDED && f.pendingOpIndex == t.opIndex()) {
                f.journal.recordValue(t.opIndex(), null);
                f.pendingOpIndex = -1;
                wakeLocked(f);
            }
        }
    }

    /** SUSPENDED -> RUNNABLE. Must hold the monitor. */
    private void wakeLocked(Fiber<?> f) {
        suspendedCount--;
        f.state = FiberState.RUNNABLE;
        ready.add(f);
    }

    /** Execute one run-attempt of the fiber body on this dispatcher thread. */
    private void runOne(Fiber<?> fiber) {
        FiberContext ctx = new FiberContext(fiber, this);
        Object value = null;
        Throwable failure = null;
        boolean suspended = false;
        try {
            value = fiber.body.run(ctx);
        } catch (SuspendControl sc) {
            suspended = true;
        } catch (FiberCancellationException c) {
            failure = c;
        } catch (Throwable t) {
            failure = t;
        }

        synchronized (this) {
            if (suspended) {
                if (fiber.yieldRequested) {
                    fiber.yieldRequested = false;
                    fiber.state = FiberState.RUNNABLE;
                    ready.add(fiber);
                } else {
                    fiber.state = FiberState.SUSPENDED;
                    suspendedCount++;
                    // Close the cancel-vs-park race: a cancellation that
                    // arrived while the body was still RUNNING must interrupt
                    // the fiber now that it is parked.
                    if (fiber.cancelRequested && fiber.pendingOpIndex >= 0) {
                        fiber.journal.recordFailure(fiber.pendingOpIndex, fiber.cancelCause);
                        fiber.pendingOpIndex = -1;
                        wakeLocked(fiber);
                    }
                }
                return;
            }
            Outcome outcome;
            if (failure instanceof FiberCancellationException c) {
                outcome = new Outcome.Cancelled<>(c);
            } else if (failure != null) {
                outcome = new Outcome.Failed<>(failure);
            } else if (fiber.cancelRequested) {
                outcome = new Outcome.Cancelled<>(fiber.cancelCause);
            } else {
                outcome = new Outcome.Success<>(value);
            }
            if (hasAliveChildrenLocked(fiber)) {
                // Structured concurrency: the parent cannot terminate until
                // every child has terminated.
                fiber.pendingOutcome = outcome;
                fiber.waitingChildren = true;
                fiber.state = FiberState.SUSPENDED;
                suspendedCount++;
                return;
            }
            finalizeLocked(fiber, outcome);
        }
    }

    private boolean hasAliveChildrenLocked(Fiber<?> fiber) {
        for (Fiber<?> c : fiber.children) {
            if (!c.state.isTerminal()) {
                return true;
            }
        }
        return false;
    }

    /** Terminal transition, exactly once. Must hold the monitor. */
    @SuppressWarnings("unchecked")
    private void finalizeLocked(Fiber<?> fiber, Outcome outcome) {
        if (fiber.state == FiberState.SUSPENDED) {
            suspendedCount--;
        }
        if (fiber.cancelRequested && !(outcome instanceof Outcome.Failed)) {
            outcome = new Outcome.Cancelled<>(fiber.cancelCause);
        }
        fiber.state = outcome instanceof Outcome.Success ? FiberState.COMPLETED
                : outcome instanceof Outcome.Failed ? FiberState.FAILED
                : FiberState.CANCELLED;
        fiber.outcome = outcome;
        registry.remove(fiber);

        if (outcome instanceof Outcome.Cancelled) {
            for (Runnable cb : fiber.cancelCallbacks) {
                try {
                    cb.run();
                } catch (Throwable ignored) {
                    // A broken callback must not harm the scheduler.
                }
            }
        }

        if (fiber.waiters != null) {
            for (Fiber.AwaitRegistration reg : fiber.waiters) {
                Fiber<?> waiter = reg.waiter();
                if (waiter.state == FiberState.SUSPENDED && waiter.pendingOpIndex == reg.opIndex()) {
                    waiter.journal.recordValue(reg.opIndex(), outcome);
                    waiter.pendingOpIndex = -1;
                    wakeLocked(waiter);
                }
            }
            fiber.waiters = null;
        }

        Fiber<?> parent = fiber.parent;
        if (parent != null && parent.waitingChildren && !parent.state.isTerminal()) {
            if (!hasAliveChildrenLocked(parent)) {
                parent.waitingChildren = false;
                finalizeLocked(parent, parent.pendingOutcome);
            }
        }
        fiber.doneLatch.countDown();
    }
}
