package com.github.guoyyit.userspacefiberruntimejava.runtime;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * API available to a fiber body. Every method is a potential suspension
 * point (cooperative scheduling) and is journaled for deterministic replay.
 *
 * <p>Replay protocol: each operation draws the next sequential index. If the
 * journal already holds a result for that index, it is returned (or thrown)
 * without re-executing; otherwise the operation executes now. A blocking
 * operation registers its completion event and unwinds the stack with
 * {@link SuspendControl} — the fiber then holds no thread.
 */
public final class FiberContext {

    private final Fiber<?> fiber;
    private final Scheduler scheduler;
    private int opIndex;

    FiberContext(Fiber<?> fiber, Scheduler scheduler) {
        this.fiber = fiber;
        this.scheduler = scheduler;
    }

    /** Memoized side effect: executed at most once, replayed afterwards. */
    @SuppressWarnings("unchecked")
    public <V> V call(Supplier<V> effect) {
        int i = nextOp();
        if (fiber.journal.isComplete(i)) {
            return (V) fiber.journal.replay(i);
        }
        checkCancelled();
        V value = effect.get();
        fiber.journal.recordValue(i, value);
        return value;
    }

    /** Park this fiber for the given duration (holds no thread). */
    public void sleep(Duration duration) {
        int i = nextOp();
        if (fiber.journal.isComplete(i)) {
            fiber.journal.replay(i);
            return;
        }
        checkCancelled();
        if (duration.isZero() || duration.isNegative()) {
            fiber.journal.recordValue(i, null);
            return;
        }
        parkAt(i, scheduler.clock().nowMillis() + duration.toMillis());
    }

    /** Voluntarily give up the current dispatch turn. */
    public void yieldNow() {
        int i = nextOp();
        if (fiber.journal.isComplete(i)) {
            fiber.journal.replay(i);
            return;
        }
        checkCancelled();
        fiber.journal.recordValue(i, null);
        fiber.yieldRequested = true;
        throw SuspendControl.INSTANCE;
    }

    /** Park indefinitely; only cancellation (or shutdown) wakes the fiber. */
    public void park() {
        int i = nextOp();
        if (fiber.journal.isComplete(i)) {
            fiber.journal.replay(i);
            return;
        }
        checkCancelled();
        scheduler.checkSuspendQuota();
        scheduler.checkStackDepth(fiber);
        fiber.journal.beginOp(i);
        synchronized (scheduler) {
            fiber.pendingOpIndex = i;
        }
        throw SuspendControl.INSTANCE;
    }

    /** Wait for another fiber to terminate and obtain its outcome value. */
    @SuppressWarnings("unchecked")
    public <V> V await(FiberHandle<V> handle) {
        int i = nextOp();
        if (fiber.journal.isComplete(i)) {
            return ((Outcome<V>) fiber.journal.replay(i)).getOrThrow();
        }
        checkCancelled();
        Fiber<V> target = (Fiber<V>) handle;
        // Terminal check + waiter registration must be atomic with respect to
        // the target's finalization, otherwise a registration could be lost.
        synchronized (scheduler) {
            Outcome<V> done = target.outcome();
            if (done != null) {
                fiber.journal.recordValue(i, done);
                return done.getOrThrow();
            }
            scheduler.checkSuspendQuota();
            scheduler.checkStackDepth(fiber);
            fiber.journal.beginOp(i);
            fiber.pendingOpIndex = i;
            scheduler.registerAwait(target, fiber, i);
        }
        throw SuspendControl.INSTANCE;
    }

    /** Spawn a child fiber (structured: this fiber waits for it). */
    @SuppressWarnings("unchecked")
    public <V> FiberHandle<V> spawn(FiberOptions options, FiberBody<V> body) {
        int i = nextOp();
        if (fiber.journal.isComplete(i)) {
            return (FiberHandle<V>) fiber.journal.replay(i);
        }
        checkCancelled();
        Fiber<V> child = scheduler.submit(fiber, options, body);
        fiber.journal.recordValue(i, child);
        return child;
    }

    /** Register a cancellation callback, guaranteed to run at most once. */
    public void onCancel(Runnable callback) {
        int i = nextOp();
        if (fiber.journal.isComplete(i)) {
            fiber.journal.replay(i);
            return;
        }
        fiber.journal.recordValue(i, null);
        synchronized (scheduler) {
            if (fiber.state.isTerminal()) {
                return;
            }
            fiber.cancelCallbacks.add(callback);
        }
    }

    /** Throw {@link FiberCancellationException} if cancellation was requested. */
    public void checkCancelled() {
        if (fiber.cancelRequested) {
            throw fiber.cancelCause;
        }
    }

    public boolean isCancelled() {
        return fiber.cancelRequested;
    }

    // ---- internals ----------------------------------------------------------

    private int nextOp() {
        return opIndex++;
    }

    private void parkAt(int opIndex, long wakeDeadlineMillis) {
        scheduler.checkSuspendQuota();
        scheduler.checkStackDepth(fiber);
        fiber.journal.beginOp(opIndex);
        synchronized (scheduler) {
            fiber.pendingOpIndex = opIndex;
        }
        scheduler.registerTimer(fiber, opIndex, wakeDeadlineMillis);
        throw SuspendControl.INSTANCE;
    }
}
