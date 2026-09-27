package com.github.guoyyit.userspacefiberruntimejava.fiber;

import java.time.Duration;

/**
 * Execution context handed to a running fiber. All blocking-looking operations are cooperative
 * suspension points: they release the worker thread while the fiber waits.
 */
public interface FiberContext {

    /** The fiber owning this context. */
    Fiber<?> fiber();

    FiberScheduler scheduler();

    /** Heap-resident local-variable table; survives every suspend/resume cycle. */
    FiberLocals locals();

    Priority priority();

    void priority(Priority priority);

    /** Resume label of the state machine (persisted across suspensions). */
    long stage();

    void stage(long stage);

    // ---- logical per-fiber stack accounting (single-stack quota) ----

    void enterFrame();

    void leaveFrame();

    int frameDepth();

    // ---- cancellation ----

    void checkCancelled();

    boolean isCancelled();

    // ---- cooperative suspension points ----

    /** Voluntarily give up the worker thread and re-enter the ready queue. */
    void yield();

    /** Suspend until an external {@link Fiber#resume()} (or cancellation). */
    void park();

    /** Suspend until resumed or until the timeout elapses. Throws on timeout. */
    void park(Duration timeout);

    /** Suspension that completes once the timeout elapses (cancellation interrupts it). */
    void sleep(Duration duration);

    // ---- structured concurrency ----

    <T> Fiber<T> spawn(FiberTask<T> task);

    <T> Fiber<T> spawn(FiberOptions options, FiberTask<T> task);

    /** Spawn a child and suspend until it completes; returns the child's result. */
    <T> T call(FiberTask<T> task) throws Exception;

    /** Suspend until the given child reaches a terminal state. */
    void join(Fiber<?> child) throws Exception;

    /** Suspend until the child terminates or the timeout elapses (throws on timeout). */
    void join(Fiber<?> child, Duration timeout) throws Exception;

    /** Suspend until every child of this fiber has terminated. */
    void joinAll() throws Exception;

    /** Suspend until all children terminate or the timeout elapses (throws on timeout). */
    void joinAll(Duration timeout) throws Exception;
}
