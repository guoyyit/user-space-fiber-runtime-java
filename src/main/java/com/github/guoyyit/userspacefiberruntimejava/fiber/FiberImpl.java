package com.github.guoyyit.userspacefiberruntimejava.fiber;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Concrete fiber: a heap-resident continuation frame.
 *
 * <p>Suspension is implemented by throwing {@link SuspendSignal}: the worker's JVM stack unwinds
 * to the scheduler loop while this object retains the stage counter, {@link FiberLocals}, frame
 * depth and all bookkeeping. Resumption re-enters {@link FiberTask#run} and the body jumps to
 * {@link #stage}. Local state therefore survives suspend/resume while the fiber occupies no
 * worker thread.
 */
final class FiberImpl<T> extends Fiber<T> implements FiberContext, ChildOwner {

    final FiberScheduler scheduler;
    final ChildOwner parent;
    final FiberTask<T> task;
    final FiberOptions options;
    final long id;
    final long seq;
    final int stackQuota;

    volatile FiberState state = FiberState.NEW;
    volatile Priority priority;
    long enqueueTime;

    long stage;
    int frameDepth;
    final FiberLocals locals = new FiberLocals();

    T result;
    volatile Throwable failure;
    volatile CancelReason cancelReason;

    /** How the fiber is currently suspended. */
    enum WaitKind { NONE, PARK, TIMED_PARK, SLEEP, JOIN }

    WaitKind waitKind = WaitKind.NONE;
    /** Pending suspension recorded by a suspend point, committed by the worker after unwinding. */
    boolean pendingSuspend;
    FiberState pendingState = FiberState.READY;
    long pendingDeadline = Long.MAX_VALUE;
    /** Guard: this fiber is currently registered in the scheduler's parked list. */
    boolean inParkedList;
    /** Timer for the current suspension (sleep/timed park); cleared on each wake. */
    TimerWheel.Entry timerEntry;
    /** Timer for the fiber's overall {@link FiberOptions#timeout()} deadline; lifetime timer. */
    TimerWheel.Entry overallTimerEntry;
    long waitDeadline = Long.MAX_VALUE;
    FiberImpl<?> joinTarget;
    boolean waitForAllChildren;

    /**
     * Outcome of the most recent suspension, consumed once at the top of the next run slice
     * before the user body is re-entered.
     */
    enum ResumeState { CONTINUE, CANCELLED, TIMED_OUT }

    ResumeState resumeState = ResumeState.CONTINUE;

    /** True when the body returned/ threw and the fiber is only waiting for children. */
    boolean bodyDone;
    boolean bodySucceeded;
    T bodyResult;
    Throwable bodyFailure;

    final List<FiberImpl<?>> children = new CopyOnWriteArrayList<>();

    /** Non-fiber thread blocked in {@link #await()} on this fiber, if any. */
    final AtomicReference<Thread> externalWaiter = new AtomicReference<>();

    final List<Runnable> cancelCallbacks = new CopyOnWriteArrayList<>();
    private volatile boolean cancelCallbacksFired;

    FiberImpl(FiberScheduler scheduler, ChildOwner parent, FiberOptions options,
              FiberTask<T> task, long id, long seq, int stackQuota) {
        this.scheduler = scheduler;
        this.parent = parent;
        this.options = options;
        this.task = task;
        this.id = id;
        this.seq = seq;
        this.priority = options.priority();
        this.stackQuota = stackQuota > 0 ? stackQuota : scheduler.config.defaultMaxStackDepth();
    }

    boolean cancelRequested() {
        return cancelReason != null;
    }

    // ================================================================== handle

    @Override public long id() { return id; }
    @Override public String name() { return options.name(); }
    @Override public FiberState state() { return state; }
    @Override public Priority priority() { return priority; }
    @Override public void priority(Priority priority) {
        if (priority != null) {
            this.priority = priority;
        }
    }

    @Override
    public boolean isDone() {
        return state.isTerminal();
    }

    @Override
    public T result() {
        return result;
    }

    @Override
    public Optional<T> resultOpt() {
        return Optional.ofNullable(result);
    }

    @Override
    public Throwable failure() {
        return failure;
    }

    @Override
    public CancelReason cancelReason() {
        return cancelReason;
    }

    @Override
    public boolean cancel(CancelReason reason) {
        return scheduler == null ? false : requestCancel(reason, true);
    }

    @Override
    public boolean resume() {
        return scheduler.wakeUp(this);
    }

    @Override
    public T await() throws Exception {
        return await(null);
    }

    @Override
    public T await(Duration timeout) throws Exception {
        // An external caller's wait is bounded by the wall clock; the (possibly manual)
        // scheduler clock governs only internal fiber suspensions, not caller threads.
        long deadline = timeout == null ? Long.MAX_VALUE
                : System.currentTimeMillis() + Math.max(0L, timeout.toMillis());
        scheduler.externalAwait(this, deadline);
        return rethrowTerminal();
    }

    private T rethrowTerminal() throws Exception {
        if (state == FiberState.COMPLETED) {
            return result;
        }
        if (state == FiberState.CANCELLED) {
            throw new FiberCancellationException(this, cancelReason != null ? cancelReason : CancelReason.MANUAL);
        }
        Throwable f = failure;
        if (f instanceof Exception e) {
            throw e;
        }
        if (f instanceof Error e) {
            throw e;
        }
        throw new FiberException("Fiber failed", f);
    }

    @Override public Fiber<?> parent() {
        return (parent instanceof FiberImpl) ? (Fiber<?>) parent : null;
    }

    @Override
    public List<Fiber<?>> children() {
        return List.copyOf(children);
    }

    @Override
    public void onCancel(Runnable callback) {
        boolean fireNow;
        synchronized (this) {
            fireNow = cancelCallbacksFired;
            if (!fireNow && callback != null) {
                cancelCallbacks.add(callback);
            }
        }
        if (fireNow && callback != null) {
            runCallbackQuietly(callback);
        }
    }

    @Override
    public FiberLocals locals() {
        return locals;
    }

    // ========================================================= cancellation

    /**
     * Request cancellation. Cascades to all descendants. Cancel callbacks fire at most once.
     * @return true if this call is the first (effective) cancellation request
     */
    boolean requestCancel(CancelReason reason, boolean cascade) {
        if (reason == null) {
            reason = CancelReason.MANUAL;
        }
        boolean first;
        List<Runnable> toFire;
        synchronized (this) {
            first = cancelReason == null && !state.isTerminal();
            if (first) {
                cancelReason = reason;
            }
            if (!cancelCallbacksFired) {
                cancelCallbacksFired = true;
                toFire = List.copyOf(cancelCallbacks);
                cancelCallbacks.clear();
            } else {
                toFire = List.of();
            }
        }
        for (Runnable cb : toFire) {
            runCallbackQuietly(cb);
        }
        if (cascade) {
            for (FiberImpl<?> child : children) {
                if (!child.state.isTerminal()) {
                    child.requestCancel(CancelReason.PARENT, true);
                }
            }
        }
        // If currently suspended, the runtime interrupts it now.
        if (state.isSuspended()) {
            scheduler.wakeUp(this);
        }
        return first;
    }

    private static void runCallbackQuietly(Runnable cb) {
        // Fault isolation: a misbehaving cancel callback must not hurt the runtime or caller.
        try {
            cb.run();
        } catch (Throwable ignored) {
            // deliberately swallowed
        }
    }

    // ================================================================== context

    @Override public Fiber<?> fiber() { return this; }
    @Override public FiberScheduler scheduler() { return scheduler; }
    @Override public long stage() { return stage; }
    @Override public void stage(long stage) { this.stage = stage; }
    @Override public int frameDepth() { return frameDepth; }

    @Override
    public void enterFrame() {
        if (++frameDepth > stackQuota) {
            frameDepth--;
            throw new StackQuotaException(
                    "Fiber '" + name() + "' exceeded single-stack frame quota of " + stackQuota);
        }
    }

    @Override
    public void leaveFrame() {
        if (frameDepth > 0) {
            frameDepth--;
        }
    }

    @Override
    public void checkCancelled() {
        if (cancelReason != null && !state.isTerminal()) {
            throw new FiberCancellationException(this, cancelReason);
        }
    }

    @Override
    public boolean isCancelled() {
        return cancelReason != null;
    }

    @Override
    public void yield() {
        checkCancelled();
        waitKind = WaitKind.NONE;
        scheduler.suspend(this, FiberState.READY, Long.MAX_VALUE);
        throw SuspendSignal.INSTANCE;
    }

    @Override
    public void park() {
        park(null);
    }

    @Override
    public void park(Duration timeout) {
        checkCancelled();
        long deadline = timeout == null ? Long.MAX_VALUE
                : scheduler.clock.nowMillis() + Math.max(0L, timeout.toMillis());
        waitKind = timeout == null ? WaitKind.PARK : WaitKind.TIMED_PARK;
        waitDeadline = deadline;
        scheduler.suspend(this,
                timeout == null ? FiberState.SUSPENDED : FiberState.TIMED_WAITING, deadline);
        throw SuspendSignal.INSTANCE;
    }

    @Override
    public void sleep(Duration duration) {
        checkCancelled();
        long millis = Math.max(0L, duration.toMillis());
        waitKind = WaitKind.SLEEP;
        waitDeadline = scheduler.clock.nowMillis() + millis;
        scheduler.suspend(this, FiberState.TIMED_WAITING, waitDeadline);
        throw SuspendSignal.INSTANCE;
    }

    @Override
    public <U> Fiber<U> spawn(FiberTask<U> task) {
        return spawn(FiberOptions.builder().build(), task);
    }

    @Override
    public <U> Fiber<U> spawn(FiberOptions options, FiberTask<U> task) {
        return spawnChild(options, task);
    }

    private <U> FiberImpl<U> spawnChild(FiberOptions options, FiberTask<U> task) {
        // createFiber registers the child with this fiber (its owner) exactly once.
        FiberImpl<U> child = scheduler.createFiber(this, options, task);
        if (cancelReason != null) {
            child.requestCancel(CancelReason.PARENT, true);
        }
        scheduler.schedule(child);
        return child;
    }

    @Override
    public <U> U call(FiberTask<U> task) throws Exception {
        FiberImpl<U> child = spawnChild(FiberOptions.builder().build(), task);
        join(child);
        return child.rethrowTerminal();
    }

    @Override
    public void join(Fiber<?> child) throws Exception {
        join(child, null);
    }

    @Override
    public void join(Fiber<?> child, Duration timeout) throws Exception {
        if (!(child instanceof FiberImpl<?> c) || c.parent != this) {
            throw new FiberException("join target is not a child of this fiber");
        }
        if (c.state.isTerminal()) {
            return;
        }
        checkCancelled();
        long deadline = timeout == null ? Long.MAX_VALUE
                : scheduler.clock.nowMillis() + Math.max(0L, timeout.toMillis());
        waitKind = WaitKind.JOIN;
        waitForAllChildren = false;
        joinTarget = c;
        waitDeadline = deadline;
        scheduler.suspend(this, FiberState.WAITING, deadline);
        throw SuspendSignal.INSTANCE;
    }

    @Override
    public void joinAll() throws Exception {
        joinAll(null);
    }

    @Override
    public void joinAll(Duration timeout) throws Exception {
        if (children.stream().allMatch(ch -> ch.state.isTerminal())) {
            return;
        }
        checkCancelled();
        long deadline = timeout == null ? Long.MAX_VALUE
                : scheduler.clock.nowMillis() + Math.max(0L, timeout.toMillis());
        waitKind = WaitKind.JOIN;
        waitForAllChildren = true;
        joinTarget = null;
        waitDeadline = deadline;
        scheduler.suspend(this, FiberState.WAITING, deadline);
        throw SuspendSignal.INSTANCE;
    }

    /** Whether the current JOIN wait condition is already met (re-checked under scheduler lock). */
    boolean joinSatisfied() {
        if (waitKind != WaitKind.JOIN) {
            return false;
        }
        if (waitForAllChildren) {
            return children.stream().allMatch(ch -> ch.state.isTerminal());
        }
        return joinTarget != null && joinTarget.state.isTerminal();
    }

    // ================================================================ owner

    @Override
    public void registerChild(FiberImpl<?> child) {
        children.add(child);
    }

    @Override
    public void childEnded(FiberImpl<?> child) {
        maybeHandleChildFailure(child);
        if (bodyDone) {
            tryFinishAfterChildren();
        } else {
            // Wake if this completion satisfied a join; if the parent is still RUNNING (about to
            // commit the suspension), finishSlice re-evaluates the condition under the lock.
            wakeJoinIfSatisfied();
        }
    }

    @Override
    public CancelReason cancellation() {
        return cancelReason;
    }

    private void maybeHandleChildFailure(FiberImpl<?> child) {
        if (child.state != FiberState.FAILED) {
            return;
        }
        ChildFailurePolicy policy = options.childFailurePolicy() != null
                ? options.childFailurePolicy()
                : scheduler.config.defaultChildFailurePolicy();
        if (policy == ChildFailurePolicy.CANCEL_SIBLINGS && cancelReason == null) {
            for (FiberImpl<?> sibling : children) {
                if (sibling != child && !sibling.state.isTerminal()) {
                    sibling.requestCancel(CancelReason.PARENT, true);
                }
            }
            // A child failure also cancels the waiting parent body.
            if (!bodyDone) {
                requestCancel(CancelReason.PARENT, false);
            }
        }
    }

    private void wakeJoinIfSatisfied() {
        boolean satisfied;
        if (waitForAllChildren) {
            satisfied = children.stream().allMatch(ch -> ch.state.isTerminal());
        } else {
            satisfied = joinTarget != null && joinTarget.state.isTerminal();
        }
        if (satisfied) {
            scheduler.wakeUp(this);
        }
    }

    private void tryFinishAfterChildren() {
        boolean allDone = children.stream().allMatch(ch -> ch.state.isTerminal());
        if (allDone) {
            scheduler.complete(this);
        }
    }

    /** Called by scheduler when a timer it armed fires while the fiber waits. */
    void onTimerFire() {
        resumeState = ResumeState.TIMED_OUT;
        scheduler.wakeUp(this);
    }

    // ========================================================= scheduler entry

    /** Execute one runnable slice. Called only on a worker thread while state == RUNNING. */
    void runSlice() {
        // Cancelled while still NEW/READY (before ever running): end without executing the body.
        if (waitKind == WaitKind.NONE && !bodyDone && cancelReason != null) {
            bodyDone = true;
            bodySucceeded = false;
            bodyFailure = new FiberCancellationException(this, cancelReason);
            settleAfterBody();
            return;
        }

        // 1) Deliver the outcome of the suspension we are resuming from.
        WaitKind kind = waitKind;
        ResumeState outcome = resumeState;
        waitKind = WaitKind.NONE;
        resumeState = ResumeState.CONTINUE;

        if (kind != WaitKind.NONE) {
            try {
                deliverResume(kind, outcome);
            } catch (SuspendSignal signal) {
                // Re-suspended (e.g. a join re-armed on a spurious wake); nothing to settle.
                return;
            } catch (RuntimeException | Error t) {
                bodyDone = true;
                bodySucceeded = false;
                bodyFailure = t;
                settleAfterBody();
                return;
            }
        }

        // 2) Re-enter the user body from the top; it jumps to the current stage.
        try {
            bodyResult = task.run(this);
            onBodyReturned();
        } catch (SuspendSignal signal) {
            // Cooperative yield: stay suspended as configured; nothing to finalize.
            return;
        } catch (Throwable t) {
            // Fault isolation: any user exception becomes a queryable FAILED state.
            bodyDone = true;
            bodySucceeded = false;
            bodyFailure = t;
            settleAfterBody();
        }
    }

    /**
     * Translate the outcome of a suspension into the effect the suspension point's contract
     * promises on resume.
     */
    private void deliverResume(WaitKind kind, ResumeState outcome) {
        boolean cancelled = cancelReason != null;

        switch (kind) {
            case JOIN -> {
                if (cancelled) {
                    throw new FiberCancellationException(this, cancelReason);
                }
                if (outcome == ResumeState.TIMED_OUT) {
                    throw new FiberTimeoutException("join timed out");
                }
                boolean ok = waitForAllChildren
                        ? children.stream().allMatch(ch -> ch.state.isTerminal())
                        : (joinTarget != null && joinTarget.state.isTerminal());
                if (!ok) {
                    // Spurious wake (e.g. another child ended); keep waiting.
                    reArmJoin();
                    throw SuspendSignal.INSTANCE;
                }
                // joinAll/ join completed normally.
            }
            case SLEEP -> {
                if (cancelled) {
                    throw new FiberCancellationException(this, cancelReason);
                }
                // A sleep's deadline firing is normal completion of the sleep, not a failure.
            }
            case TIMED_PARK -> {
                if (cancelled) {
                    throw new FiberCancellationException(this, cancelReason);
                }
                if (outcome == ResumeState.TIMED_OUT) {
                    throw new FiberTimeoutException("park timed out");
                }
                // Explicit resume before deadline: continue normally.
            }
            case PARK -> {
                if (cancelled) {
                    throw new FiberCancellationException(this, cancelReason);
                }
                // Explicit resume: continue normally.
            }
            default -> {
                // YIELD or NONE: a cancellation still interrupts.
                if (cancelled) {
                    throw new FiberCancellationException(this, cancelReason);
                }
            }
        }
    }

    private void reArmJoin() {
        waitKind = WaitKind.JOIN;
        // Preserve the original join deadline so a spurious wake cannot lose a timeout.
        scheduler.suspend(this, FiberState.WAITING, waitDeadline);
    }

    private void onBodyReturned() {
        bodyDone = true;
        bodySucceeded = true;
        settleAfterBody();
    }

    private void settleAfterBody() {
        if (bodySucceeded) {
            result = bodyResult;
        } else {
            failure = bodyFailure;
        }
        boolean allChildrenDone = children.stream().allMatch(ch -> ch.state.isTerminal());
        if (allChildrenDone) {
            scheduler.complete(this);
        } else {
            // Body finished first: keep the fiber alive (WAITING) until all children end.
            waitKind = WaitKind.JOIN;
            waitForAllChildren = true;
            joinTarget = null;
            waitDeadline = Long.MAX_VALUE;
            timerEntry = null;
            state = FiberState.WAITING;
            // Account as parked while waiting for children.
            scheduler.suspendParkedOnly(this);
        }
    }

    /** Decide the terminal outcome once the body and every child are done. */
    void decideTerminal() {
        if (cancelReason != null) {
            state = FiberState.CANCELLED;
            if (failure == null) {
                failure = new FiberCancellationException(this, cancelReason);
            }
            return;
        }
        if (bodySucceeded) {
            state = FiberState.COMPLETED;
            result = bodyResult;
            return;
        }
        // Failed body, or (with CONTINUE policy) a failed child surfaced at join.
        state = FiberState.FAILED;
        if (failure == null) {
            failure = bodyFailure;
        }
    }
}
