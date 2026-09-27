package com.github.guoyyit.userspacefiberruntimejava.fiber;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A structured-concurrency boundary anchored on an ordinary (non-fiber) caller thread.
 *
 * <p>Every fiber spawned through the scope is its child. {@link #close()} blocks until all
 * children have terminated; {@link #cancel()} cascades to every descendant and unblocks a
 * blocked close. If a child fails with an exception the configured {@link ChildFailurePolicy}
 * applies (by default surviving siblings are cancelled); {@code close} then surfaces the first
 * failure. Fibers spawned by another fiber are tracked by that fiber instead, so no child can
 * escape as an orphan.
 */
public final class FiberScope implements AutoCloseable, ChildOwner {

    private final FiberScheduler scheduler;
    private final ChildFailurePolicy failurePolicy;
    private final List<FiberImpl<?>> children = new CopyOnWriteArrayList<>();
    private final Object gate = new Object();
    private volatile boolean closed;
    private volatile CancelReason cancelled;
    private volatile Throwable firstFailure;

    FiberScope(FiberScheduler scheduler, ChildFailurePolicy failurePolicy) {
        this.scheduler = scheduler;
        this.failurePolicy = failurePolicy != null
                ? failurePolicy
                : scheduler.config.defaultChildFailurePolicy();
    }

    public static FiberScope open(FiberScheduler scheduler) {
        return scheduler.openScope(null);
    }

    public static FiberScope open(FiberScheduler scheduler, ChildFailurePolicy policy) {
        return scheduler.openScope(policy);
    }

    public FiberScheduler scheduler() {
        return scheduler;
    }

    public List<Fiber<?>> children() {
        return List.copyOf(children);
    }

    public boolean isCancelled() {
        return cancelled != null;
    }

    public <T> Fiber<T> spawn(FiberTask<T> task) {
        return spawn(FiberOptions.builder().build(), task);
    }

    public <T> Fiber<T> spawn(FiberOptions options, FiberTask<T> task) {
        if (closed) {
            throw new FiberException("scope already closed");
        }
        // createFiber registers the child with this scope (its owner) exactly once.
        FiberImpl<T> child = scheduler.createFiber(this, options, task);
        if (cancelled != null) {
            child.requestCancel(CancelReason.PARENT, true);
        }
        scheduler.schedule(child);
        return child;
    }

    public boolean cancel() {
        boolean first;
        synchronized (gate) {
            first = cancelled == null;
            if (first) {
                cancelled = CancelReason.MANUAL;
            }
        }
        for (FiberImpl<?> child : children) {
            if (!child.state.isTerminal()) {
                child.requestCancel(CancelReason.PARENT, true);
            }
        }
        wakeAll();
        return first;
    }

    /** Block the caller thread until every child terminates; throw if any child failed. */
    @Override
    public void close() {
        close(null);
    }

    public void close(Duration timeout) {
        // The caller thread's timeout is wall-clock based; the scheduler clock is only for
        // internal fiber suspensions.
        long deadline = (timeout == null || timeout.toMillis() == Long.MAX_VALUE)
                ? Long.MAX_VALUE
                : System.currentTimeMillis() + Math.max(0L, timeout.toMillis());
        synchronized (gate) {
            closed = true;
            while (!allChildrenDone()) {
                long wait = 0L;
                if (deadline != Long.MAX_VALUE) {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0L) {
                        break;
                    }
                    wait = Math.min(remaining, 50L);
                } else {
                    wait = 50L;
                }
                try {
                    gate.wait(wait);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (!allChildrenDone()) {
            // Timed out while children still run: cancel them and report the timeout.
            cancel();
            throw new FiberTimeoutException("scope close timed out with "
                    + children.stream().filter(c -> !c.state.isTerminal()).count()
                    + " live children");
        }
        Throwable failure = firstFailure;
        // Only the fail-fast policy surfaces errors at close; CONTINUE keeps failures queryable.
        if (failure != null && failurePolicy == ChildFailurePolicy.CANCEL_SIBLINGS) {
            if (failure instanceof RuntimeException re) {
                throw re;
            }
            if (failure instanceof Error er) {
                throw er;
            }
            throw new FiberException("scope completed with a failed child", failure);
        }
    }

    // ============================================================ ChildOwner

    @Override
    public void registerChild(FiberImpl<?> child) {
        children.add(child);
    }

    @Override
    public void childEnded(FiberImpl<?> child) {
        if (child.state == FiberState.FAILED && firstFailure == null) {
            firstFailure = child.failure();
            if (failurePolicy == ChildFailurePolicy.CANCEL_SIBLINGS) {
                for (FiberImpl<?> sibling : children) {
                    if (sibling != child && !sibling.state.isTerminal()) {
                        sibling.requestCancel(CancelReason.PARENT, true);
                    }
                }
            }
        }
        wakeAll();
    }

    @Override
    public CancelReason cancellation() {
        return cancelled;
    }

    // ================================================================ helpers

    private boolean allChildrenDone() {
        return children.stream().allMatch(c -> c.state.isTerminal());
    }

    private void wakeAll() {
        synchronized (gate) {
            gate.notifyAll();
        }
    }
}
